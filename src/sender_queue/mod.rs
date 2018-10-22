//! # Sender queue
//!
//! A sender queue allows a `DistAlgorithm` that outputs `Epoched` messages to buffer those outgoing
//! messages based on their epochs. A message is sent to its recipient only when the recipient's
//! epoch matches the epoch of the message. Thus no queueing is required for incoming messages since
//! any incoming messages with non-matching epochs can be safely discarded.

mod message;

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;

use rand::Rand;
use serde::{Deserialize, Serialize};

use {DistAlgorithm, Epoched, KnowsAllRemoteNodes, NetworkInfo, NodeIdT, Target, TargetedMessage};

pub use self::message::{Message, MessageContent};

/// Generic sender queue functionality.
pub trait SenderQueueFunc<D>
where
    D: DistAlgorithm,
    <D as DistAlgorithm>::Message: Clone + Epoched + Serialize + for<'r> Deserialize<'r>,
    D::NodeId: NodeIdT + Rand,
{
    type Step;

    /// Returns the maximum epoch out of a given epoch and the epoch of a given batch.
    fn max_epoch_with_batch(
        &self,
        epoch: <D::Message as Epoched>::Epoch,
        batch: &D::Output,
    ) -> <D::Message as Epoched>::Epoch;

    /// Whether the epoch `them` accepts the message `us`.
    fn is_accepting_epoch(&self, us: &D::Message, them: <D::Message as Epoched>::Epoch) -> bool;

    /// Whether the epoch `them` is ahead of the epoch of the message `us`.
    fn is_later_epoch(&self, us: &D::Message, them: <D::Message as Epoched>::Epoch) -> bool;

    /// Whether the message should be sent immediately rather than postponed.
    fn is_passed_unchanged(
        &self,
        msg: &TargetedMessage<D::Message, D::NodeId>,
        remote_epochs: &BTreeMap<D::NodeId, <D::Message as Epoched>::Epoch>,
    ) -> bool {
        let pass =
            |&them: &<D::Message as Epoched>::Epoch| self.is_accepting_epoch(&msg.message, them);
        match &msg.target {
            Target::All => remote_epochs.values().all(pass),
            Target::Node(id) => remote_epochs.get(&id).map_or(false, pass),
        }
    }

    /// A _spanning epoch_ of an epoch `e` is an epoch `e0` such that
    ///
    /// - `e` and `e0` are incomparable by the partial ordering on epochs and
    ///
    /// - the duration of `e0` is at least that of `e`.
    ///
    /// Returned is a list of spanning epochs for the given epoch.
    ///
    /// For example, any `DynamicHoneyBadger` epoch `Epoch((x, Some(y)))` has a unique spanning
    /// epoch `Epoch((x, None))`. In turn, no epoch `Epoch((x, None))` has a spanning epoch.
    fn spanning_epochs(
        epoch: <D::Message as Epoched>::Epoch,
    ) -> Vec<<D::Message as Epoched>::Epoch>;
}

pub type OutgoingQueue<D> = BTreeMap<
    (
        <D as DistAlgorithm>::NodeId,
        <<D as DistAlgorithm>::Message as Epoched>::Epoch,
    ),
    Vec<<D as DistAlgorithm>::Message>,
>;

/// An instance of `DistAlgorithm` wrapped with a queue of outgoing messages, that is, a sender
/// queue. This wrapping ensures that the messages sent to remote instances lead to progress of the
/// entire consensus network. In particular, messages to lagging remote nodes are queued and sent
/// only when those nodes' epochs match the queued messages' epochs. Thus all nodes can handle
/// incoming messages without queueing them and can ignore messages whose epochs are not currently
/// acccepted.
#[derive(Debug)]
pub struct SenderQueue<D>
where
    D: DistAlgorithm + SenderQueueFunc<D>,
    D::Message: Clone + Epoched + Serialize + for<'r> Deserialize<'r>,
    D::NodeId: NodeIdT + Rand,
{
    /// The managed `DistAlgorithm` instance.
    algo: D,
    /// `NetworkInfo` of the managed algorithm.
    netinfo: Arc<NetworkInfo<D::NodeId>>,
    /// Current epoch.
    epoch: <D::Message as Epoched>::Epoch,
    /// Messages that couldn't be handled yet by remote nodes.
    outgoing_queue: OutgoingQueue<D>,
    /// Known current epochs of remote nodes.
    remote_epochs: BTreeMap<D::NodeId, <D::Message as Epoched>::Epoch>,
}

pub type Step<D> = ::Step<SenderQueue<D>>;

pub type Result<T, D> = ::std::result::Result<T, <D as DistAlgorithm>::Error>;

impl<D> DistAlgorithm for SenderQueue<D>
where
    D: DistAlgorithm + Debug + Send + Sync + SenderQueueFunc<D> + KnowsAllRemoteNodes<D>,
    D::Message: Clone + Epoched + Serialize + for<'r> Deserialize<'r>,
    D::NodeId: NodeIdT + Rand,
{
    type NodeId = D::NodeId;
    type Input = D::Input;
    type Output = D::Output;
    type Message = Message<D::Message>;
    type Error = D::Error;

    fn handle_input(&mut self, input: Self::Input) -> Result<Step<D>, D> {
        let mut step = self.algo.handle_input(input)?;
        let mut sender_queue_step = self.update_epoch(&step);
        self.defer_messages(&mut step);
        sender_queue_step.extend(step.map(|output| output, Message::from));
        Ok(sender_queue_step)
    }

    fn handle_message(
        &mut self,
        sender_id: &D::NodeId,
        message: Self::Message,
    ) -> Result<Step<D>, D> {
        match message.content {
            MessageContent::EpochStarted => Ok(self.handle_epoch_started(sender_id, message.epoch)),
            MessageContent::Algo(msg) => self.handle_message_content(sender_id, msg),
        }
    }

    fn terminated(&self) -> bool {
        false
    }

    fn our_id(&self) -> &D::NodeId {
        self.netinfo.our_id()
    }
}

impl<D> SenderQueue<D>
where
    D: DistAlgorithm + Debug + Send + Sync + SenderQueueFunc<D> + KnowsAllRemoteNodes<D>,
    D::Message: Clone + Epoched + Serialize + for<'r> Deserialize<'r>,
    D::NodeId: NodeIdT + Rand,
{
    /// Returns a new `SenderQueueBuilder` configured to manage a given `DynamicHoneyBadger` instance.
    pub fn builder(algo: D) -> SenderQueueBuilder<D> {
        SenderQueueBuilder::new(algo)
    }

    /// Handles an epoch start announcement.
    fn handle_epoch_started(
        &mut self,
        sender_id: &D::NodeId,
        epoch: <D::Message as Epoched>::Epoch,
    ) -> Step<D> {
        self.remote_epochs
            .entry(sender_id.clone())
            .and_modify(|e| {
                if *e < epoch {
                    *e = epoch;
                }
            }).or_insert(epoch);
        self.remove_earlier_messages(sender_id, epoch);
        self.process_new_epoch(sender_id, epoch)
    }

    /// Removes all messages queued for the remote node from epochs upto `epoch`.
    fn remove_earlier_messages(
        &mut self,
        sender_id: &D::NodeId,
        epoch: <D::Message as Epoched>::Epoch,
    ) {
        let earlier_keys: Vec<_> = self
            .outgoing_queue
            .keys()
            .cloned()
            .filter(|(id, this_epoch)| {
                id == sender_id
                    && PartialOrd::partial_cmp(this_epoch, &epoch) == Some(Ordering::Less)
            }).collect();
        for key in earlier_keys {
            self.outgoing_queue.remove(&key);
        }
    }

    /// Processes an announcement of a new epoch update received from a remote node.
    fn process_new_epoch(
        &mut self,
        sender_id: &D::NodeId,
        epoch: <D::Message as Epoched>::Epoch,
    ) -> Step<D> {
        // Send any HB messages for the HB epoch.
        let mut ready_messages = self
            .outgoing_queue
            .remove(&(sender_id.clone(), epoch))
            .unwrap_or_else(|| vec![]);
        for u in <D as SenderQueueFunc<D>>::spanning_epochs(epoch) {
            // Send any DHB messages for the DHB era.
            ready_messages.extend(
                self.outgoing_queue
                    .remove(&(sender_id.clone(), u))
                    .unwrap_or_else(|| vec![]),
            );
        }
        Step::from(ready_messages.into_iter().map(|msg| {
            Target::Node(sender_id.clone()).message(Message {
                epoch: msg.epoch(),
                content: MessageContent::Algo(msg),
            })
        }))
    }

    /// Handles a Honey Badger algorithm message in a given epoch.
    fn handle_message_content(
        &mut self,
        sender_id: &D::NodeId,
        content: D::Message,
    ) -> Result<Step<D>, D> {
        let mut step = self.algo.handle_message(sender_id, content)?;
        let mut sender_queue_step = self.update_epoch(&step);
        self.defer_messages(&mut step);
        sender_queue_step.extend(step.map(|output| output, Message::from));
        Ok(sender_queue_step)
    }

    /// Updates the current Honey Badger epoch.
    fn update_epoch(&mut self, step: &::Step<D>) -> Step<D> {
        let mut updated = false;
        // Look up `DynamicHoneyBadger` epoch updates.
        self.epoch = step.output.iter().fold(self.epoch, |epoch, batch| {
            let max_epoch = self.algo.max_epoch_with_batch(epoch, batch);
            if max_epoch != epoch {
                updated = true;
            }
            max_epoch
        });
        if updated {
            // Announce the new epoch.
            Target::All
                .message(MessageContent::EpochStarted.with_epoch(self.epoch))
                .into()
        } else {
            Step::default()
        }
    }

    /// Removes any messages to nodes at earlier epochs from the given `Step`. This may involve
    /// decomposing a `Target::All` message into `Target::Node` messages and sending some of the
    /// resulting messages while placing onto the queue those remaining messages whose recipient is
    /// currently at an earlier epoch.
    fn defer_messages(&mut self, step: &mut ::Step<D>) {
        let remote_epochs = &self.remote_epochs;
        let algo = &mut self.algo;
        let all_remote_nodes = algo.all_remote_nodes();
        let all_remote_nodes_cloned = all_remote_nodes.iter().cloned();
        let deferred_msgs = step.defer_messages(
            &self.remote_epochs,
            all_remote_nodes_cloned,
            |us, them| algo.is_accepting_epoch(us, them),
            |us, them| algo.is_later_epoch(us, them),
            |msg| algo.is_passed_unchanged(msg, remote_epochs),
        );
        // Append the deferred messages onto the queues.
        for (id, message) in deferred_msgs {
            let epoch = message.epoch();
            self.outgoing_queue
                .entry((id, epoch))
                .and_modify(|e| e.push(message.clone()))
                .or_insert_with(|| vec![message.clone()]);
        }
    }

    /// Returns a reference to the managed algorithm.
    pub fn algo(&self) -> &D {
        &self.algo
    }
}

/// A builder of a Honey Badger with a sender queue. It configures the parameters and creates a new
/// instance of `SenderQueue`.
pub struct SenderQueueBuilder<D>
where
    D: DistAlgorithm,
    D::Message: Epoched,
{
    algo: D,
    epoch: <D::Message as Epoched>::Epoch,
    outgoing_queue: OutgoingQueue<D>,
    remote_epochs: BTreeMap<D::NodeId, <D::Message as Epoched>::Epoch>,
}

impl<D> SenderQueueBuilder<D>
where
    D: DistAlgorithm + Debug + Send + Sync + SenderQueueFunc<D> + KnowsAllRemoteNodes<D>,
    D::Message: Clone + Epoched + Serialize + for<'r> Deserialize<'r>,
    D::NodeId: NodeIdT + Rand,
{
    pub fn new(algo: D) -> Self {
        SenderQueueBuilder {
            algo,
            epoch: <D::Message as Epoched>::Epoch::default(),
            outgoing_queue: BTreeMap::default(),
            remote_epochs: BTreeMap::default(),
        }
    }

    pub fn epoch(mut self, epoch: <D::Message as Epoched>::Epoch) -> Self {
        self.epoch = epoch;
        self
    }

    pub fn outgoing_queue(mut self, outgoing_queue: OutgoingQueue<D>) -> Self {
        self.outgoing_queue = outgoing_queue;
        self
    }

    pub fn remote_epochs(
        mut self,
        remote_epochs: BTreeMap<D::NodeId, <D::Message as Epoched>::Epoch>,
    ) -> Self {
        self.remote_epochs = remote_epochs;
        self
    }

    pub fn build(self, netinfo: Arc<NetworkInfo<D::NodeId>>) -> (SenderQueue<D>, Step<D>) {
        let epoch = <D::Message as Epoched>::Epoch::default();
        let sq = SenderQueue {
            algo: self.algo,
            netinfo,
            epoch: self.epoch,
            outgoing_queue: self.outgoing_queue,
            remote_epochs: self.remote_epochs,
        };
        let step: Step<D> = Target::All
            .message(MessageContent::EpochStarted.with_epoch(epoch))
            .into();
        (sq, step)
    }
}

use std::cmp;

use rand::Rand;
use serde::{Deserialize, Serialize};

use super::{Batch, HoneyBadger, Message, Step};
use sender_queue::SenderQueueFunc;
use {Contribution, Epoched, NodeIdT};

impl<C, N> SenderQueueFunc<HoneyBadger<C, N>> for HoneyBadger<C, N>
where
    C: Contribution + Serialize + for<'r> Deserialize<'r>,
    N: NodeIdT + Serialize + for<'r> Deserialize<'r> + Rand,
{
    type Step = Step<C, N>;

    fn max_epoch_with_batch(&self, epoch: u64, batch: &Batch<C, N>) -> u64 {
        cmp::max(batch.epoch + 1, epoch)
    }

    fn is_accepting_epoch(&self, us: &Message<N>, them: u64) -> bool {
        let our_epoch = us.epoch();
        them <= our_epoch && our_epoch <= them + self.max_future_epochs
    }

    fn is_later_epoch(&self, us: &Message<N>, them: u64) -> bool {
        us.epoch() < them
    }

    fn spanning_epochs(_epoch: u64) -> Vec<u64> {
        vec![]
    }
}

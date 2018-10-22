use std::cmp;

use rand::Rand;
use serde::{Deserialize, Serialize};

use super::{Batch, Epoch, Message, QueueingHoneyBadger, Step};
use sender_queue::SenderQueueFunc;
use transaction_queue::TransactionQueue;
use {Contribution, Epoched, NodeIdT};

impl<T, N, Q> SenderQueueFunc<QueueingHoneyBadger<T, N, Q>> for QueueingHoneyBadger<T, N, Q>
where
    T: Contribution + Serialize + for<'r> Deserialize<'r> + Clone,
    N: NodeIdT + Serialize + for<'r> Deserialize<'r> + Rand,
    Q: TransactionQueue<T>,
{
    type Step = Step<T, N, Q>;

    fn max_epoch_with_batch(&self, epoch: Epoch, batch: &Batch<T, N>) -> Epoch {
        cmp::max(*batch.next_epoch(), epoch)
    }

    fn is_accepting_epoch(&self, us: &Message<N>, Epoch((them_era, them_hb_epoch)): Epoch) -> bool {
        let Epoch((era, hb_epoch)) = us.epoch();
        era == them_era
            && (them_hb_epoch <= hb_epoch
                && hb_epoch <= them_hb_epoch.map(|e| e + self.dyn_hb.max_future_epochs() as u64))
    }

    fn is_later_epoch(&self, us: &Message<N>, Epoch((them_era, them_hb_epoch)): Epoch) -> bool {
        let Epoch((era, hb_epoch)) = us.epoch();
        era < them_era || (era == them_era && hb_epoch.is_some() && hb_epoch < them_hb_epoch)
    }

    fn spanning_epochs(epoch: Epoch) -> Vec<Epoch> {
        if let Epoch((era, Some(_))) = epoch {
            vec![Epoch((era, None))]
        } else {
            vec![]
        }
    }
}

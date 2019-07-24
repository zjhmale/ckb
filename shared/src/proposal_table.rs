use crate::ProposalView;
use ckb_chain_spec::consensus::ProposalWindow;
use ckb_core::header::BlockNumber;
use ckb_core::transaction::ProposalShortId;
use ckb_logger::trace_target;
use std::collections::{BTreeMap, HashSet};
use std::ops::Bound;

#[derive(Debug, PartialEq, Clone, Eq)]
pub struct ProposalTable {
    pub(crate) table: BTreeMap<BlockNumber, HashSet<ProposalShortId>>,
    pub(crate) proposal_window: ProposalWindow,
}

impl ProposalTable {
    pub fn new(proposal_window: ProposalWindow) -> Self {
        ProposalTable {
            proposal_window,
            table: BTreeMap::default(),
        }
    }

    // If the TABLE did not have this value present, true is returned.
    // If the TABLE did have this value present, false is returned
    pub fn insert(&mut self, number: BlockNumber, ids: HashSet<ProposalShortId>) -> bool {
        self.table.insert(number, ids).is_none()
    }

    pub fn remove(&mut self, number: BlockNumber) -> Option<HashSet<ProposalShortId>> {
        self.table.remove(&number)
    }

    pub fn all(&self) -> &BTreeMap<BlockNumber, HashSet<ProposalShortId>> {
        &self.table
    }

    pub fn finalize(
        &mut self,
        origin: &ProposalView,
        number: BlockNumber,
    ) -> (HashSet<ProposalShortId>, ProposalView) {
        let proposal_start = number.saturating_sub(self.proposal_window.farthest()) + 1;
        let proposal_end = number.saturating_sub(self.proposal_window.closest()) + 1;

        let mut left = self.table.split_off(&proposal_start);
        ::std::mem::swap(&mut self.table, &mut left);

        trace_target!(
            crate::LOG_TARGET_CHAIN,
            "[proposal_finalize] table {:?}",
            self.table
        );
        let new_ids = self
            .table
            .range((Bound::Unbounded, Bound::Included(&proposal_end)))
            .map(|pair| pair.1)
            .cloned()
            .flatten()
            .collect();

        let gap = self
            .table
            .range((Bound::Excluded(&proposal_end), Bound::Unbounded))
            .map(|pair| pair.1)
            .cloned()
            .flatten()
            .collect();

        let removed_ids: HashSet<ProposalShortId> =
            origin.set().difference(&new_ids).cloned().collect();
        trace_target!(
            crate::LOG_TARGET_CHAIN,
            "[proposal_finalize] number {} proposal_start {}----proposal_end {}",
            number,
            proposal_start,
            proposal_end
        );
        trace_target!(
            crate::LOG_TARGET_CHAIN,
            "[proposal_finalize] number {} new_ids {:?}----removed_ids {:?}",
            number,
            new_ids,
            removed_ids
        );
        (removed_ids, ProposalView::new(gap, new_ids))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_finalize() {
        let id = ProposalShortId::zero();
        let window = ProposalWindow(2, 10);
        let mut table = ProposalTable::new(window);
        let mut ids = HashSet::default();
        ids.insert(id);
        table.insert(1, ids.clone());
        assert!(!table.contains(&id));

        // in window
        for i in 2..10 {
            assert!(table.finalize(i).is_empty());
            assert!(table.contains(&id));
        }

        assert_eq!(table.finalize(11), ids);
        assert!(!table.contains(&id));

        assert!(table.finalize(12).is_empty());
        assert!(!table.contains(&id));
    }
}

use linked_hash_map::LinkedHashMap;
use crate::types::PageId;

pub trait ReplacementStrategy: Send + Sync {
    fn update(&mut self, page_id: PageId);  // TODO: add error checking if applicable
    fn get_evict<'a>(&'a mut self) -> Box<dyn Iterator<Item = PageId> + 'a>;
}

pub enum ReplacementStrategyType {
    LRU
}

pub fn replacement_strategy_factory(
    strategy_type: ReplacementStrategyType
) -> Box<dyn ReplacementStrategy + Send + Sync> {
    match strategy_type {
        ReplacementStrategyType::LRU => Box::new(LRUReplacementStrategy {
            map: LinkedHashMap::new()
        })
    }
}

/// LRU replacement strategy
pub struct LRUReplacementStrategy {
    map: LinkedHashMap<PageId, ()>,
}

impl ReplacementStrategy for LRUReplacementStrategy {
    fn update(&mut self, page_id: PageId) {
        self.map.remove(&page_id);
        self.map.insert(page_id, ());
    }

    fn get_evict<'a>(&'a mut self) -> Box<dyn Iterator<Item = PageId> + 'a> {
        Box::new(self.map.keys().copied())
    }
}
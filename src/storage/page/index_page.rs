use crate::storage::page::page::Page;
use crate::types::{PageId, SlotId, PAGE_SIZE};

type KeysLen = u16;
const PAGE_ID_SIZE: usize = size_of::<PageId>();
const SLOT_ID_SIZE: usize = size_of::<SlotId>();
const KEYS_LEN: usize = size_of::<KeysLen>();
pub const fn get_page_header_size() -> usize {
    PAGE_ID_SIZE + 1 + KEYS_LEN
}
// maximum number of leaf nodes an index page can hold
pub const fn get_leaf_capacity() -> usize {
    (PAGE_SIZE - get_page_header_size() - PAGE_ID_SIZE - 1) /
        (PAGE_ID_SIZE + SLOT_ID_SIZE + size_of::<i64>())
}
// maximum number of internal nodes an index page can hold
pub const fn get_internal_capacity() -> usize {
    (PAGE_SIZE - get_page_header_size() - PAGE_ID_SIZE) /
        (size_of::<i64>() + PAGE_ID_SIZE)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordId {
    pub page_id: PageId,
    pub slot_id: SlotId,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum IndexType {
    Leaf = 0,
    Internal = 1,
}

impl IndexType {
    fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(IndexType::Leaf),
            1 => Some(IndexType::Internal),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct IndexPage {
    id: PageId,
    pub page_type: IndexType,
    pub keys: Vec<i64>,
    rids: Vec<RecordId>,   // only for leaf node
    children: Vec<PageId>, // only for internal node
    next: Option<PageId>,  // only for leaf node
}

impl IndexPage {
    pub fn new(id: PageId, page_type: IndexType) -> Self {
        Self {
            id,
            page_type,
            keys: Vec::new(),
            rids: Vec::new(),
            children: Vec::new(),
            next: None,
        }
    }

    #[inline]
    pub fn get_rids(&self) -> &Vec<RecordId> {
        debug_assert_eq!(self.page_type, IndexType::Leaf);
        &self.rids
    }

    #[inline]
    pub fn get_children(&self) -> &Vec<PageId> {
        debug_assert_eq!(self.page_type, IndexType::Internal);
        &self.children
    }

    #[inline]
    pub fn get_children_mut(&mut self) -> &mut Vec<PageId> {
        debug_assert_eq!(self.page_type, IndexType::Internal);
        &mut self.children
    }

    /// For internal page: find child page following given key
    pub fn search_child(&self, key: &i64) -> Option<PageId> {
        debug_assert_eq!(self.page_type, IndexType::Internal);
        for (i, k) in self.keys.iter().enumerate() {
            if key < k {
                return self.children.get(i).copied();
            }
        }
        // otherwise go to the rightmost child
        self.children.last().copied()
    }

    /// For leaf pages: return exact match RID if exists.
    pub fn search_rid(&self, key: &i64) -> Option<&RecordId> {
        debug_assert_eq!(self.page_type, IndexType::Leaf);
        self.keys
            .iter()
            .position(|k| k == key)
            .map(|i| &self.rids[i])
    }

    /// For leaf page: insert a key into index page
    pub fn insert_record(&mut self, key: i64, record: RecordId) {
        debug_assert_eq!(self.page_type, IndexType::Leaf);
        match self.keys.binary_search(&key) {
            Ok(index) => self.rids[index] = record,
            Err(index) => {
                self.keys.insert(index, key);
                self.rids.insert(index, record);
            }
        }
    }

    /// For internal page: insert a child into index page
    pub fn insert_child(&mut self, key: i64, child: PageId) {
        debug_assert_eq!(self.page_type, IndexType::Internal);
        let index = self.keys.binary_search(&key).unwrap_or_else(|pos| pos);
        self.keys.insert(index, key);
        self.children.insert(index + 1, child);
        debug_assert_eq!(self.children.len(), self.keys.len() + 1);
    }

    /// Remove a key with associated child/record
    /// Return error if the element is not found
    pub fn remove_key(&mut self, key: i64) -> bool {
        if let Ok(index) = self.keys.binary_search(&key) {
            self.keys.remove(index);
            match self.page_type {
                IndexType::Internal => {
                    self.children.remove(index);
                }
                IndexType::Leaf => {
                    self.rids.remove(index);
                }
            }
            true
        } else {
            false
        }
    }

    /// For leaf pages: return RIDs within a key range.
    /// The range scan only covers the current page. For the entire key range
    /// compare maximum key with target end and continue scanning self.next
    pub fn search_range(&self, start: &i64, end: &i64) -> Vec<&RecordId> {
        debug_assert_eq!(self.page_type, IndexType::Leaf);
        self.keys
            .iter()
            .zip(&self.rids)
            .filter_map(|(k, rid)| {
                if k >= start && k <= end {
                    Some(rid)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Split page into two, return the key to be promoted and sibling page
    pub fn split(&mut self, new_id: PageId) -> (i64, Self) {
        let mid = self.keys.len() / 2;

        match self.page_type {
            IndexType::Internal => {
                let promoted_key = self.keys[mid];
                let sibling_keys = self.keys.split_off(mid + 1);
                let sibling_children = self.children.split_off(mid + 1);

                let mut sibling = IndexPage::new(new_id, IndexType::Internal);
                sibling.keys = sibling_keys;
                sibling.children = sibling_children;
                (promoted_key, sibling)
            }
            IndexType::Leaf => {
                let promoted_key = self.keys[mid];
                let sibling_keys = self.keys.split_off(mid);
                let sibling_rids = self.rids.split_off(mid);

                let mut sibling = IndexPage::new(new_id, IndexType::Leaf);
                sibling.keys = sibling_keys;
                sibling.rids = sibling_rids;
                sibling.next = self.next.take();
                self.next = Some(sibling.id);
                (promoted_key, sibling)
            }
        }
    }

    /// Merge sibling into one page
    pub fn merge(&mut self, sibling: &mut Self) {
        debug_assert_eq!(self.page_type, sibling.page_type);

        match self.page_type {
            IndexType::Internal => {
                self.keys.extend(&sibling.keys);
                self.children.extend(&sibling.children);
            }
            IndexType::Leaf => {
                self.keys.extend(&sibling.keys);
                self.rids.extend(&sibling.rids);
                self.next = sibling.next.take();
            }
        }
    }

    /// Redistribute keys between self and sibling and return the new separator key for parent
    /// Returns: (new_separator_key, borrowed_key) where new_separator_key should update parent
    pub fn redistribute(&mut self, sibling: &mut Self, borrow_from_left: bool, min_keys: usize) -> Option<i64> {
        debug_assert_eq!(self.page_type, sibling.page_type);

        // check the sibling has enough keys to borrow from
        if sibling.keys.len() <= min_keys {
            return None;
        }

        match self.page_type {
            IndexType::Internal => {
                if borrow_from_left {
                    // take last key from left sibling
                    let key = sibling.keys.pop().unwrap();
                    let child = sibling.children.pop().unwrap();
                    self.keys.insert(0, key);
                    self.children.insert(0, child);

                    // the new separator for parent should be the last key in sibling
                    sibling.keys.last().copied()
                } else {
                    // take first key from right sibling
                    let key = sibling.keys.remove(0);
                    let child = sibling.children.remove(0);
                    self.keys.push(key);
                    self.children.push(child);

                    // the new separator for parent should be the first key in sibling
                    sibling.keys.first().copied()
                }
            }
            IndexType::Leaf => {
                if borrow_from_left {
                    // take last key from left sibling
                    let key = sibling.keys.pop().unwrap();
                    let rid = sibling.rids.pop().unwrap();
                    self.keys.insert(0, key);
                    self.rids.insert(0, rid);

                    // for leaf nodes, the new separator is the first key in self (the borrowed one)
                    self.keys.first().copied()
                } else {
                    // take first key from right sibling
                    let key = sibling.keys.remove(0);
                    let rid = sibling.rids.remove(0);
                    self.keys.push(key);
                    self.rids.push(rid);

                    // for leaf nodes borrowing from right, the new separator is the borrowed key
                    sibling.keys.first().copied()
                }
            }
        }
    }

    /// For leat node: set next sibling
    pub fn set_next(&mut self, next: PageId) {
        debug_assert_eq!(self.page_type, IndexType::Leaf);
        self.next = Some(next);
    }

    /// For leaf node: get next sibling
    pub fn get_next(&self) -> Option<PageId> {
        debug_assert_eq!(self.page_type, IndexType::Leaf);
        self.next
    }

    pub fn min_key(&self) -> Option<i64> {
        self.keys.first().copied()
    }

    pub fn max_key(&self) -> Option<i64> {
        self.keys.last().copied()
    }
}

impl Page for IndexPage {

    /// new: default to leaf page
    fn new(id: PageId) -> Self {
        IndexPage::new(id, IndexType::Leaf)
    }

    /// Internal Page Layout:
    /// [id: 4][page_type: 1][keys.len(): 4][key: children]
    /// Leaf Page Layout:
    /// [id: 4][page_type: 1][keys.len(): 4][has_next: 1][next: 4][keys: rids[PageId, SlotId]]
    fn serialize(&self) -> [u8; PAGE_SIZE] {
        let mut buf = [0u8; PAGE_SIZE];
        let mut cursor = 0;

        // serialize page header
        buf[cursor..cursor + PAGE_ID_SIZE].copy_from_slice(&self.id.to_le_bytes());
        cursor += PAGE_ID_SIZE;
        buf[cursor] = self.page_type as u8;
        cursor += 1;
        buf[cursor..cursor + KEYS_LEN].copy_from_slice(&(self.keys.len() as KeysLen).to_le_bytes());
        cursor += KEYS_LEN;

        match self.page_type {
            IndexType::Internal => {
                // write key-child pairs
                // children.len() = keys.len() + 1
                for (i, k) in self.keys.iter().enumerate() {
                    buf[cursor..cursor + size_of::<i64>()].copy_from_slice(&k.to_le_bytes());
                    cursor += size_of::<i64>();
                    buf[cursor..cursor + PAGE_ID_SIZE].copy_from_slice(&self.children[i].to_le_bytes());
                    cursor += PAGE_ID_SIZE;
                }
                // write last child
                if let Some(last) = self.children.last() {
                    buf[cursor..cursor + PAGE_ID_SIZE].copy_from_slice(&last.to_le_bytes());
                    cursor += PAGE_ID_SIZE;
                }

            },
            IndexType::Leaf => {
                // write next node
                if let Some(next) = self.next {
                    buf[cursor] = 1;
                    cursor += 1;
                    buf[cursor..cursor + PAGE_ID_SIZE].copy_from_slice(&next.to_le_bytes());
                    cursor += PAGE_ID_SIZE;
                } else {
                    buf[cursor..cursor + 1 + PAGE_ID_SIZE].fill(0);
                    cursor += 1 + PAGE_ID_SIZE;
                }
                // write key-rid pairs
                for (k, rid) in self.keys.iter().zip(&self.rids) {
                    buf[cursor..cursor + size_of::<i64>()].copy_from_slice(&k.to_le_bytes());
                    cursor += size_of::<i64>();
                    buf[cursor..cursor + PAGE_ID_SIZE].copy_from_slice(&rid.page_id.to_le_bytes());
                    cursor += PAGE_ID_SIZE;
                    buf[cursor..cursor + SLOT_ID_SIZE].copy_from_slice(&rid.slot_id.to_le_bytes());
                    cursor += SLOT_ID_SIZE;
                }
            }
        }
        buf
    }

    fn deserialize(buf: &[u8; PAGE_SIZE]) -> Option<Self> {
        let mut cursor = 0;

        // deserialize page header
        let id = PageId::from_le_bytes(buf[cursor..cursor + PAGE_ID_SIZE].try_into().ok()?);
        cursor += PAGE_ID_SIZE;
        let page_type = IndexType::from_byte(buf[cursor])?;
        cursor += 1;
        let keys_len = KeysLen::from_le_bytes(buf[cursor..cursor + KEYS_LEN].try_into().ok()?);
        cursor += KEYS_LEN;

        let mut keys = Vec::with_capacity(keys_len as usize);
        let mut rids = Vec::with_capacity(keys_len as usize);
        let mut children = Vec::with_capacity((keys_len + 1) as usize);
        let mut next = None;

        match page_type {
            IndexType::Internal => {
                // deserialize key-child pairs
                for _ in 0..keys_len {
                    let key = i64::from_le_bytes(buf[cursor..cursor + size_of::<i64>()].try_into().ok()?);
                    cursor += size_of::<i64>();
                    let child = PageId::from_le_bytes(buf[cursor..cursor + PAGE_ID_SIZE].try_into().ok()?);
                    cursor += PAGE_ID_SIZE;
                    keys.push(key);
                    children.push(child);
                }
                // deserialize last child
                if keys_len > 0 {
                    let child = PageId::from_le_bytes(buf[cursor..cursor + PAGE_ID_SIZE].try_into().ok()?);
                    cursor += PAGE_ID_SIZE;
                    children.push(child);
                }
            },
            IndexType::Leaf => {
                // deserialize next node
                let has_next = buf[cursor] == 1;
                cursor += 1;
                if has_next {
                    next = Some(PageId::from_le_bytes(buf[cursor..cursor + PAGE_ID_SIZE].try_into().ok()?));
                }
                cursor += PAGE_ID_SIZE;
                // deserialize key-rid pairs
                for _ in 0..keys_len {
                    let key = i64::from_le_bytes(buf[cursor..cursor + size_of::<i64>()].try_into().ok()?);
                    cursor += size_of::<i64>();
                    let page_id = PageId::from_le_bytes(buf[cursor..cursor + PAGE_ID_SIZE].try_into().ok()?);
                    cursor += PAGE_ID_SIZE;
                    let slot_id = SlotId::from_le_bytes(buf[cursor..cursor + SLOT_ID_SIZE].try_into().ok()?);
                    cursor += SLOT_ID_SIZE;
                    keys.push(key);
                    rids.push(RecordId { page_id, slot_id });
                }
            },
        }

        Some(IndexPage {
            id,
            page_type,
            keys,
            rids,
            children,
            next,
        })
    }

    #[inline]
    fn get_id(&self) -> PageId {
        self.id
    }

    fn get_free_space(&self) -> usize {
        // PAGE_SIZE - used
        let mut used = get_page_header_size();
        match self.page_type {
            IndexType::Leaf => {
                used += self.keys.len() * (size_of::<i64>() + PAGE_ID_SIZE + SLOT_ID_SIZE); // key + rid
                used += PAGE_ID_SIZE + 1;   // next + has_next
            }
            IndexType::Internal => {
                used += self.keys.len() * (size_of::<i64>() + PAGE_ID_SIZE); // key + child
                used += PAGE_ID_SIZE;   // last child
            }
        }
        PAGE_SIZE - used
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::page::page::Page;

    fn create_record(page: PageId, slot: SlotId) -> RecordId {
        RecordId { page_id: page, slot_id: slot }
    }

    #[test]
    fn test_insert_and_search_rid() {
        let mut page = IndexPage::new(1, IndexType::Leaf);
        let rid = create_record(2, 3);
        page.insert_record(42, rid);

        assert_eq!(page.keys, vec![42]);
        assert_eq!(page.search_rid(&42), Some(&rid));
        assert_eq!(page.search_rid(&99), None);
    }

    #[test]
    fn test_insert_duplicate_records() {
        let mut page = IndexPage::new(1, IndexType::Leaf);
        let rid1 = create_record(2, 3);
        let rid2 = create_record(4, 5);
        page.insert_record(42, rid1);
        page.insert_record(42, rid2);

        assert_eq!(page.keys, vec![42]);
        assert_eq!(page.search_rid(&42), Some(&rid2));
    }

    #[test]
    fn test_insert_child_and_search_child() {
        let mut page = IndexPage::new(1, IndexType::Internal);
        page.children.push(10); // leftmost child
        page.insert_child(50, 20);
        page.insert_child(100, 30);

        // search < 50
        assert_eq!(page.search_child(&25), Some(10));
        // search between 50 and 100
        assert_eq!(page.search_child(&75), Some(20));
        // search >= 100
        assert_eq!(page.search_child(&150), Some(30));
    }

    #[test]
    fn test_remove_key_leaf() {
        let mut page = IndexPage::new(1, IndexType::Leaf);
        page.insert_record(1, create_record(2, 1));
        page.insert_record(2, create_record(2, 2));
        page.insert_record(3, create_record(2, 3));

        assert!(page.remove_key(2));
        assert_eq!(page.keys, vec![1, 3]);
        assert!(!page.remove_key(999));
    }

    #[test]
    fn test_split_leaf() {
        let mut page = IndexPage::new(1, IndexType::Leaf);
        for i in 0..6 {
            page.insert_record(i, create_record(1, i as SlotId));
        }

        let (promoted, sibling) = page.split(2);

        assert!(promoted >= 2 && promoted <= 3);
        assert_eq!(page.keys.len() + sibling.keys.len(), 6);
        assert_eq!(page.get_next(), Some(2));
        assert_eq!(sibling.get_next(), None);
    }

    #[test]
    fn test_split_internal() {
        let mut page = IndexPage::new(1, IndexType::Internal);
        page.children.push(0);
        for i in 0..5 {
            page.insert_child(i * 10, (i + 1) as PageId);
        }

        let (promoted, sibling) = page.split(2);

        assert!(page.keys.len() >= 2 && page.keys.len() <= 5);
        assert!(sibling.keys.len() >= 2 && sibling.keys.len() <= 5);
        assert!(promoted == 20 || promoted == 30);
        assert_eq!(page.children.len() + sibling.children.len(), 6);
    }

    #[test]
    fn test_merge_leaf() {
        let mut left = IndexPage::new(1, IndexType::Leaf);
        left.insert_record(1, create_record(1, 1));
        let mut right = IndexPage::new(2, IndexType::Leaf);
        right.insert_record(2, create_record(2, 2));

        left.merge(&mut right);
        assert_eq!(left.keys, vec![1, 2]);
        assert_eq!(left.rids.len(), 2);
        assert_eq!(right.keys.len(), 0); // consumed
    }

    #[test]
    fn test_redistribute_leaf() {
        let mut left = IndexPage::new(1, IndexType::Leaf);
        let mut right = IndexPage::new(2, IndexType::Leaf);

        left.insert_record(1, create_record(1, 1));
        left.insert_record(2, create_record(1, 2));
        right.insert_record(10, create_record(2, 1));

        // borrow from left
        let sep = right.redistribute(&mut left, true, 1);
        assert!(sep.is_some());
        assert!(right.keys.first().unwrap() <= &10);
    }

    #[test]
    fn test_serialize_deserialize_leaf() {
        let mut page = IndexPage::new(1, IndexType::Leaf);
        page.insert_record(42, create_record(7, 11));
        page.set_next(99);

        let buf = page.serialize();
        let deserialized = IndexPage::deserialize(&buf).unwrap();

        assert_eq!(page.id, deserialized.id);
        assert_eq!(page.page_type, deserialized.page_type);
        assert_eq!(page.keys, deserialized.keys);
        assert_eq!(page.rids, deserialized.rids);
        assert_eq!(page.get_next(), deserialized.get_next());
    }

    #[test]
    fn test_serialize_deserialize_internal() {
        let mut page = IndexPage::new(1, IndexType::Internal);
        page.children.push(5);
        page.insert_child(42, 6);

        let buf = page.serialize();
        let deserialized = IndexPage::deserialize(&buf).unwrap();

        assert_eq!(page.id, deserialized.id);
        assert_eq!(page.page_type, deserialized.page_type);
        assert_eq!(page.keys, deserialized.keys);
        assert_eq!(page.children, deserialized.children);
    }

    #[test]
    fn test_free_space_calculation() {
        let mut page = IndexPage::new(1, IndexType::Leaf);
        let free0 = page.get_free_space();

        page.insert_record(1, create_record(1, 1));
        let free1 = page.get_free_space();

        assert!(free1 < free0);
    }
}
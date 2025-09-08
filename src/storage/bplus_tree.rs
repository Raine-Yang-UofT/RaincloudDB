use paste::paste;
use std::sync::Arc;
use crate::types::PageId;
use crate::storage::bufferpool::BufferPool;
use crate::storage::index_page::{IndexPage, IndexType, RecordId};
use crate::storage::page::Page;
use crate::{with_create_pages, with_read_pages, with_write_pages};

pub struct BPlusTree {
    root: PageId,
    buffer_pool: Arc<BufferPool<IndexPage>>,
    internal_max_keys: usize,
    leaf_max_keys: usize,
    internal_min_keys: usize,
    leaf_min_keys: usize,
}

impl BPlusTree {
    pub fn new(root: PageId,
               buffer_pool: Arc<BufferPool<IndexPage>>,
               internal_max_keys: usize,
               leaf_max_keys: usize
    ) -> BPlusTree {
        // TODO validate max keys with page size

        BPlusTree {
            root,
            buffer_pool,
            internal_max_keys,
            leaf_max_keys,
            internal_min_keys: internal_max_keys / 2,
            leaf_min_keys: leaf_max_keys / 2,
        }
    }

    /// Search record by key
    pub fn search(&mut self, key: &i64) -> Option<RecordId> {
        let mut curr_id = self.root;
        loop {
            with_read_pages!(self, [(curr_id, curr_page)], false, {
                match curr_page.page.page_type {
                    IndexType::Internal => {
                        curr_id = curr_page.page.search_child(key)?;
                    }
                    IndexType::Leaf => {
                        return curr_page.page.search_rid(key).cloned();
                    }
                }
            });
        }
    }

    /// Insert (key, rid). Split pages if exceed bound
    pub fn insert(&mut self, key: i64, rid: RecordId) {
        let mut stack = self.descend_to_leaf(key);

        // Step 1: insert into leaf
        let leaf_id = stack.pop().expect("Error: leaf node not found");
        let mut promote: Option<(i64, PageId)> = None;

        with_write_pages!(self, [(leaf_id, leaf_page)], true, {
            leaf_page.page.insert_record(key, rid);

            // split leaf if overflow
            if leaf_page.page.keys.len() > self.leaf_max_keys {
                let mut sib_id = 0;
                with_create_pages!(self, [(sib_id, sib_page)], true, {
                    sib_id = sib_page.page.get_id();
                    let (promoted_key, new_sibling_page) = leaf_page.page.split(sib_id);
                    sib_page.page = new_sibling_page;
                    promote = Some((promoted_key, sib_id));
                });
            }
        });

        // Step 2: propagate promotion upward
        while let Some((promoted_key, promoted_child)) = promote.take() {
            if let Some(parent_id) = stack.pop() {
                with_write_pages!(self, [(parent_id, parent_page)], true, {
                    parent_page.page.insert_child(promoted_key, promoted_child);

                    // split parent if exceeds capacity
                    if parent_page.page.keys.len() > self.internal_max_keys {
                        let mut sib_id = 0;
                        with_create_pages!(self, [(sib_id, sib_page)], true, {
                            sib_id = sib_page.page.get_id();
                            let (promoted_key, sibling_page) = parent_page.page.split(sib_id);

                            sib_page.page.page_type = IndexType::Internal;
                            sib_page.page = sibling_page;
                            promote = Some((promoted_key, sib_id));
                        });
                    } else {
                        // insertion complete, no further promotion
                        promote = None;
                    }

                });
            } else {
                // no parent: create new root
                let mut root_id = 0;
                with_create_pages!(self, [(root_id, root_page)], true, {
                    root_id = root_page.page.get_id();
                    root_page.page.page_type = IndexType::Internal;
                    root_page.page.get_children_mut().push(self.root);
                    root_page.page.insert_child(promoted_key, promoted_child);

                    self.root = root_id;
                    promote = None;
                });
            }
        }
        // save changes
        self.buffer_pool.flush_all();
    }

    /// Delete given key. Use redistribution and merge.
    /// Return true if deletion succeed
    pub fn delete(&mut self, key: i64) -> bool {
        let mut stack = self.descend_to_leaf(key);

        // Step 1: delete from leaf
        let leaf_id = stack.pop().unwrap();
        if let Ok(frame) = self.buffer_pool.fetch_page(leaf_id) {
            let mut fg = frame.write().unwrap();
            let removed = fg.page.remove_key(key);
            // no key removed
            if !removed {
                self.buffer_pool.unpin_page(leaf_id, false).expect("Error: unpin page failed");
                return false;
            }
            // key successfully removed
            if fg.page.keys.len() >= self.leaf_min_keys {
                self.buffer_pool.unpin_page(leaf_id, true).expect("Error: unpin page failed");
                return true;
            }
            self.buffer_pool.unpin_page(leaf_id, true).expect("Error: unpin page failed");
        }

        // Step 2: Attempt to fix underflow with redistribution
        let mut child_id = leaf_id;
        let mut is_leaf = true;    // is current layer the leaf layer

        while let Some(parent_id) = stack.pop() {
            if let (Ok(parent_frame), Ok(child_frame)) =
                (self.buffer_pool.fetch_page(parent_id), self.buffer_pool.fetch_page(child_id))
            {
                let mut parent_fg = parent_frame.write().unwrap();
                let mut child_fg = child_frame.write().unwrap();

                let index = parent_fg.page.get_children().iter()
                    .position(|&id| id == child_id)
                    .expect("Error: child not found in parent");

                // try borrow from left sibling, update parent separator using returned key
                if index > 0 {
                    let left_id = parent_fg.page.get_children()[index - 1];
                    if let Ok(left_frame) = self.buffer_pool.fetch_page(left_id) {
                        // attempt to redistribute: borrow from left into child
                        let mut left_fg = left_frame.write().unwrap();
                        if let Some(new_separator) = child_fg.page.redistribute(
                            // redistribute succeed, set new separator to parent keys
                            &mut left_fg.page,
                            true,
                            if is_leaf { self.leaf_min_keys } else { self.internal_min_keys }
                        ) {
                            parent_fg.page.keys[index - 1] = new_separator;
                            self.buffer_pool.unpin_page(child_id, true).expect("Error: unpin page failed");
                            self.buffer_pool.unpin_page(parent_id, true).expect("Error: unpin page failed");
                            self.buffer_pool.unpin_page(left_id, true).expect("Error: unpin page failed");
                            return true;
                        }
                    }
                }

                // try borrow from right sibling
                if index < parent_fg.page.get_children().len() - 1 {
                    let right_id = parent_fg.page.get_children()[index + 1];
                    if let Ok(right_frame) = self.buffer_pool.fetch_page(right_id) {
                        let mut right_fg = right_frame.write().unwrap();
                        if let Some(new_separator) = child_fg.page.redistribute(
                            &mut right_fg.page,
                            false,
                        if is_leaf { self.leaf_min_keys } else { self.internal_min_keys }
                        ) {
                            parent_fg.page.keys[index] = new_separator;
                            self.buffer_pool.unpin_page(child_id, true).expect("Error: unpin page failed");
                            self.buffer_pool.unpin_page(parent_id, true).expect("Error: unpin page failed");
                            self.buffer_pool.unpin_page(right_id, true).expect("Error: unpin page failed");
                            return true;
                        }
                    }
                }

                // cannot borrow: merge with sibling
                // attempt to merge with left sibling if exists, otherwise right sibling.
                if index > 0 {
                    let left_id = parent_fg.page.get_children()[index - 1];
                    if let Ok(left_frame) = self.buffer_pool.fetch_page(left_id) {
                        let mut left_fg = left_frame.write().unwrap();

                        if is_leaf {
                            // For leaf: remove separator between merged pages
                            parent_fg.page.keys.remove(index - 1);
                        } else {
                            // For internal: bring parent separator down to left_page before merging
                            let separator = parent_fg.page.keys.remove(index - 1);
                            left_fg.page.keys.push(separator);
                        }

                        // merge child to left page
                        left_fg.page.merge(&mut child_fg.page);
                        parent_fg.page.get_children_mut().remove(index);
                        // TODO: free the child page entirely
                        self.buffer_pool.unpin_page(left_id, true).expect("Error: unpin page failed");
                    }
                } else {
                    // merge with right sibling
                    let right_id = parent_fg.page.get_children()[index + 1];
                    if let Ok(right_frame) = self.buffer_pool.fetch_page(right_id) {
                        let mut right_fg = right_frame.write().unwrap();

                        if is_leaf {
                            // For leaf: remove separator between merged pages
                            parent_fg.page.keys.remove(index);
                        } else {
                            // For internal: bring parent separator down to child_page before merging
                            let separator = parent_fg.page.keys.remove(index - 1);
                            child_fg.page.keys.push(separator);
                        }

                        // merge right into child page
                        child_fg.page.merge(&mut right_fg.page);
                        parent_fg.page.get_children_mut().remove(index + 1);
                        // TODO: free right page entirely
                        self.buffer_pool.unpin_page(child_id, true).expect("Error: unpin page failed");
                    }
                }

                // propagate update if parent is underflow
                child_id = parent_id;
                if parent_fg.page.keys.len() >= self.leaf_min_keys {
                    self.buffer_pool.unpin_page(parent_id, true).expect("Error: unpin page failed");
                    break;
                } else {
                    self.buffer_pool.unpin_page(parent_id, true).expect("Error: unpin page failed");
                    continue;
                }
            }

            is_leaf = false;
        }

        // root collapse: replace root with child if root only has one element
        if let Ok(root_frame) = self.buffer_pool.fetch_page(self.root) {
            let mut root_fg = root_frame.write().unwrap();
            if root_fg.page.keys.is_empty() && root_fg.page.get_children().len() == 1 {
                let new_root = root_fg.page.get_children()[0];
                self.root = new_root;
                // TODO: free root page entirely
            }
        }

        true
    }

    /// Find given key in leaf page
    fn descend_to_leaf(&self, key: i64) -> Vec::<PageId> {
        let mut stack: Vec<PageId> = Vec::new();
        let mut curr_id = self.root;

        loop {
            if let Ok(frame) = self.buffer_pool.fetch_page(curr_id) {
                let fg = frame.read().unwrap();
                match fg.page.page_type {
                    IndexType::Internal => {
                        let child = fg.page.search_child(&key).expect("Error: internal search child failed");
                        stack.push(curr_id);
                        curr_id = child;
                    }
                    IndexType::Leaf => {
                        stack.push(curr_id);
                        break;
                    }
                }
                self.buffer_pool.unpin_page(curr_id, false).expect("Error: unpin page failed");
            }
        }
        stack
    }
}
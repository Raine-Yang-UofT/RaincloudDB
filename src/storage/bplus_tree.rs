use paste::paste;
use std::sync::Arc;
use crate::types::PageId;
use crate::storage::bufferpool::BufferPool;
use crate::storage::index_page::{get_internal_capacity, get_leaf_capacity, IndexPage, IndexType, RecordId};
use crate::{with_create_pages, with_read_pages, with_write_pages};
use crate::storage::page::Page;

/// B+ Tree Invariant: left keys < parent separator <= right keys
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
        debug_assert!(internal_max_keys > 0);
        debug_assert!(leaf_max_keys > 0);
        debug_assert!(internal_max_keys < get_internal_capacity());
        debug_assert!(leaf_max_keys < get_leaf_capacity());

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
            with_read_pages!(self, [(curr_id, curr_page)], {
                match curr_page.page_type {
                    IndexType::Internal => {
                        curr_id = curr_page.search_child(key)?;
                    }
                    IndexType::Leaf => {
                        return curr_page.search_rid(key).cloned();
                    }
                }
            });
        }
    }

    /// Insert (key, rid). Split pages if exceed bound
    pub fn insert(&mut self, key: i64, rid: RecordId) {

        let root_id = self.root;

        // early insert if tree is empty (leaf root)
        with_write_pages!(self, [(root_id, root_page)], true, {
            if root_page.page_type == IndexType::Leaf && root_page.keys.is_empty() {
                root_page.insert_record(key, rid);
                return;
            }
        });

        let mut stack = self.descend_to_leaf(key);

        // Step 1: insert into leaf
        let leaf_id = stack.pop().expect("Error: leaf node not found");
        let mut promote: Option<(i64, PageId)> = None;

        with_write_pages!(self, [(leaf_id, leaf_page)], true, {
            leaf_page.insert_record(key, rid);

            // split leaf if overflow
            if leaf_page.keys.len() > self.leaf_max_keys {
                let sib_id;
                with_create_pages!(self, [(sib_id, sib_page)], true, {
                    let (promoted_key, new_sibling_page) = leaf_page.split(sib_id);
                    *sib_page = new_sibling_page;
                    promote = Some((promoted_key, sib_id));
                });
            }
        });

        // Step 2: propagate promotion upward
        while let Some((promoted_key, promoted_child)) = promote.take() {
            if let Some(parent_id) = stack.pop() {

                with_write_pages!(self, [(parent_id, parent_page)], true, {
                    parent_page.insert_child(promoted_key, promoted_child);

                    // split parent if exceeds capacity
                    if parent_page.keys.len() > self.internal_max_keys {
                        let sib_id;
                        with_create_pages!(self, [(sib_id, sib_page)], true, {
                            let (promoted_key, sibling_page) = parent_page.split(sib_id);
                            *sib_page = sibling_page;
                            sib_page.page_type = IndexType::Internal;
                            promote = Some((promoted_key, sib_id));
                        });
                    }
                });
            } else {
                // no parent: create new root
                let mut root_id = 0;
                with_create_pages!(self, [(root_id, root_page)], true, {
                    root_page.page_type = IndexType::Internal;
                    root_page.get_children_mut().push(self.root);
                    root_page.insert_child(promoted_key, promoted_child);
                    self.root = root_id;
                });
            }
        }
    }

    /// Delete given key. Use redistribution and merge.
    /// Return true if deletion succeed
    pub fn delete(&mut self, key: i64) -> bool {

        // if the tree is empty, there is no node to delete
        let root_id = self.root;
        with_read_pages!(self, [(root_id, root_page)], {
            if root_page.page_type == IndexType::Leaf && root_page.keys.is_empty() {
                return false;
            }
        });

        // Step 1: delete from leaf
        let mut stack = self.descend_to_leaf(key);
        let leaf_id = stack.pop().unwrap();
        with_write_pages!(self, [(leaf_id, leaf_page)], true, {
            let removed = leaf_page.remove_key(key);
            if !removed {
                return false;
            }
            // key successfully removed
            if leaf_page.keys.len() >= self.leaf_min_keys {
                return true;
            }
        });

        let mut child_id = leaf_id;
        let mut is_leaf = true;    // is current layer the leaf layer

        while let Some(parent_id) = stack.pop() {
            with_write_pages!(self, [(parent_id, parent_page), (child_id, child_page)], true, {
                let index = parent_page.get_children().iter()
                    .position(|&id| id == child_id)
                    .expect("Error: child not found in parent");

                // Step 2: Attempt to fix underflow with redistribution
                // try borrow from left sibling, update parent separator using returned key
                if index > 0 {
                    let left_id = parent_page.get_children()[index - 1];

                    with_write_pages!(self, [(left_id, left_page)], true, {
                        if let Some(new_separator) = child_page.redistribute(
                            // redistribute succeed, set new separator to parent keys
                            &mut left_page,
                            true,
                            if is_leaf { self.leaf_min_keys } else { self.internal_min_keys }
                        ) {
                            parent_page.keys[index - 1] = new_separator;
                            return true;
                        }
                    });
                } else if index < parent_page.get_children().len() - 1 {
                    // try borrow from right sibling
                    let right_id = parent_page.get_children()[index + 1];

                    with_write_pages!(self, [(right_id, right_page)], true, {
                        if let Some(new_separator) = child_page.redistribute(
                            &mut right_page,
                            false,
                            if is_leaf { self.leaf_min_keys } else { self.internal_min_keys }
                        ) {
                            parent_page.keys[index] = new_separator;
                            return true;
                        }
                    });
                }

                // Step 3: merge with sibling
                // attempt to merge with left sibling if exists, otherwise right sibling.
                if index > 0 {
                    let left_id = parent_page.get_children()[index - 1];

                    with_write_pages!(self, [(left_id, left_page)], true, {
                        // check if merge is possible (combined size doesn't exceed max)
                        let max_keys = if is_leaf { self.leaf_max_keys } else { self.internal_max_keys };
                        let combined_size = left_page.keys.len() + child_page.keys.len() +
                                                    if is_leaf { 0 } else { 1 }; // +1 for separator in internal nodes
                        if combined_size > max_keys {
                            // the tree structure is invalid
                            panic!("Unable to fix underflow: no redistribution or merge possible");
                        }

                        if is_leaf {
                            // for leaf: remove separator, append child to left
                            parent_page.keys.remove(index - 1);
                        } else {
                            // for internal: bring parent separator down to left_page before merging
                            let separator = parent_page.keys.remove(index - 1);
                        }

                        // merge child to left page
                        left_page.merge(&mut child_page);
                        parent_page.get_children_mut().remove(index);
                        // TODO: free the child page entirely
                    });
                } else if index < parent_page.get_children().len() - 1 {
                    // merge with right sibling
                    let right_id = parent_page.get_children()[index + 1];
                    with_write_pages!(self, [(right_id, right_page)], true, {
                        // check if merge is possible (combined size doesn't exceed max)
                        let max_keys = if is_leaf { self.leaf_max_keys } else { self.internal_max_keys };
                        let combined_size = right_page.keys.len() + child_page.keys.len() +
                                                    if is_leaf { 0 } else { 1 }; // +1 for separator in internal nodes
                        if combined_size > max_keys {
                            // the tree structure is invalid
                            panic!("Unable to fix underflow: no redistribution or merge possible");
                        }

                        if is_leaf {
                            // for leaf: remove separator between merged pages
                            parent_page.keys.remove(index);
                        } else {
                            // for internal: bring parent separator down to child_page before merging
                            let separator = parent_page.keys.remove(index);
                            child_page.keys.push(separator);
                        }

                        // merge right into child page
                        child_page.merge(&mut right_page);
                        parent_page.get_children_mut().remove(index + 1);
                        // TODO: free right page entirely
                    });
                }

                // propagate update if parent is underflow
                child_id = parent_id;
                let parent_min = match parent_page.page_type {
                    IndexType::Leaf => self.leaf_min_keys,
                    IndexType::Internal => self.internal_min_keys,
                };
                if parent_page.keys.len() >= parent_min {
                    break;
                }
                is_leaf = false;
            });
        }

        // root collapse: replace root with child if root only has one element
        let root_id = self.root;
        with_write_pages!(self, [(root_id, root_page)], true, {
            match root_page.page_type {
                IndexType::Internal => {
                    // internal root should collapse when it has no keys (and thus only 1 child)
                    if root_page.keys.is_empty() {
                        debug_assert_eq!(root_page.get_children().len(), 1,
                                "Internal root with no keys should have exactly 1 child");

                        let new_root = root_page.get_children()[0];
                        // TODO: free old root page entirely
                        self.root = new_root;
                    }
                }
                IndexType::Leaf => {
                    if root_page.keys.is_empty() {
                        // do nothing here
                    }
                }
            }
        });

        true
    }

    /// Find given key in leaf page
    fn descend_to_leaf(&self, key: i64) -> Vec::<PageId> {
        let mut stack: Vec<PageId> = Vec::new();
        let mut curr_id = self.root;

        loop {
            with_read_pages!(self, [(curr_id, curr_page)], {
                match curr_page.page_type {
                    IndexType::Internal => {
                        let child = curr_page.search_child(&key).expect("Error: internal search child failed");
                        stack.push(curr_id);
                        curr_id = child;
                    }
                    IndexType::Leaf => {
                        stack.push(curr_id);
                        break;
                    }
                }
            });
        }
        stack
    }
}
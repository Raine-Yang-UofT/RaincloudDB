use paste::paste;
use std::sync::Arc;
use crate::types::{PageId, FLUSH};
use crate::storage::bufferpool::BufferPool;
use crate::storage::page::index_page::{get_internal_capacity, get_leaf_capacity, IndexPage, IndexType, RecordId};
use crate::{with_create_pages, with_read_pages, with_write_pages};
use crate::storage::page::page::Page;

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
            with_read_pages!(self.buffer_pool, [(curr_id, curr_page)], {
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
        self.print_tree();
        println!("insert {key}");

        // early insert if tree is empty (leaf root)
        let mut insertion_complete = false;
        with_write_pages!(self.buffer_pool, [(root_id, root_page)], FLUSH, {
            if root_page.page_type == IndexType::Leaf && root_page.keys.is_empty() {
                root_page.insert_record(key, rid);
                insertion_complete = true;
            }
        });
        if insertion_complete { return }

        let mut stack = self.descend_to_leaf(key);

        // Step 1: insert into leaf
        let leaf_id = stack.pop().expect("Error: leaf node not found");
        let mut promote: Option<(i64, PageId)> = None;

        with_write_pages!(self.buffer_pool, [(leaf_id, leaf_page)], FLUSH, {
            leaf_page.insert_record(key, rid);

            // split leaf if overflow
            if leaf_page.keys.len() > self.leaf_max_keys {
                let sib_id;
                with_create_pages!(self.buffer_pool, [(sib_id, sib_page)], FLUSH, {
                    let (promoted_key, new_sibling_page) = leaf_page.split(sib_id);
                    *sib_page = new_sibling_page;
                    promote = Some((promoted_key, sib_id));
                });
            }
        });

        // Step 2: propagate promotion upward
        while let Some((promoted_key, promoted_child)) = promote.take() {
            if let Some(parent_id) = stack.pop() {
                with_write_pages!(self.buffer_pool, [(parent_id, parent_page)], FLUSH, {
                    parent_page.insert_child(promoted_key, promoted_child);

                    // split parent if exceeds capacity
                    if parent_page.keys.len() > self.internal_max_keys {
                        let sib_id;
                        with_create_pages!(self.buffer_pool, [(sib_id, sib_page)], FLUSH, {
                            let (promoted_key, sibling_page) = parent_page.split(sib_id);
                            *sib_page = sibling_page;
                            sib_page.page_type = IndexType::Internal;
                            promote = Some((promoted_key, sib_id));
                        });
                    }
                });
            } else {
                // no parent: create new root
                let root_id;
                with_create_pages!(self.buffer_pool, [(root_id, root_page)], FLUSH, {
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
        self.print_tree();
        println!("delete {key}");
        // if the tree is empty, there is no node to delete
        let root_id = self.root;
        let mut tree_empty = false;
        with_read_pages!(self.buffer_pool, [(root_id, root_page)], {
            if root_page.page_type == IndexType::Leaf && root_page.keys.is_empty() {
                tree_empty = true;
            }
        });
        if tree_empty { return false }

        // Step 1: delete from leaf
        let mut stack = self.descend_to_leaf(key);
        let leaf_id = stack.pop().unwrap();
        let mut underflow_node = None;

        let mut deletion_failed = false;
        with_write_pages!(self.buffer_pool, [(leaf_id, leaf_page)], FLUSH, {
            if !leaf_page.remove_key(key) {
                deletion_failed = true;
            }
            // key successfully removed
            if leaf_page.keys.len() < self.leaf_min_keys {
                underflow_node = Some(leaf_id);
            }
        });
        if deletion_failed { return false }

        while let Some(child_id) = underflow_node.take() {
            if let Some(parent_id) = stack.pop() {
                with_write_pages!(self.buffer_pool, [(parent_id, parent_page), (child_id, child_page)], FLUSH, {
                    let index = parent_page.get_children()
                        .iter()
                        .position(|&id| id == child_id)
                        .expect("Error: child not found in parent");
                    let min_keys = if child_page.page_type == IndexType::Leaf { self.leaf_min_keys } else { self.internal_min_keys };

                    // Step 2: Attempt to fix underflow with redistribution
                    // try borrow from left sibling, update parent separator using returned key
                    let left_sibling = if index > 0 { Some(parent_page.get_children()[index - 1]) } else { None };
                    let right_sibling = if index < parent_page.get_children().len() - 1
                        {Some(parent_page.get_children()[index + 1])} else { None };

                    // try to redistribute from left sibling
                    let mut redistribute_succeed = false;
                    if let Some(left_id) = left_sibling {
                        with_write_pages!(self.buffer_pool, [(left_id, left_page)], FLUSH, {
                            let old_sep = parent_page.keys[index - 1];
                            if let Some(new_sep) = child_page.redistribute(&mut left_page, old_sep, true, min_keys) {
                                parent_page.keys[index - 1] = new_sep;
                                redistribute_succeed = true;
                            }
                        });
                    }
                    // try to redistribute from right sibling
                    if let Some(right_id) = right_sibling {
                        with_write_pages!(self.buffer_pool, [(right_id, right_page)], FLUSH, {
                            let old_sep = parent_page.keys[index];
                            if let Some(new_sep) = child_page.redistribute(&mut right_page, old_sep, false, min_keys) {
                                parent_page.keys[index] = new_sep;
                                redistribute_succeed = true;
                            }
                        });
                    }
                    if redistribute_succeed { return true }

                    // Step 3: merge with sibling
                    // Merge with left sibling if possible
                    if let Some(left_id) = left_sibling {
                        with_write_pages!(self.buffer_pool, [(left_id, left_page)], FLUSH, {
                            let sep_key = parent_page.keys.remove(index - 1); // remove parent separator
                            left_page.merge(&mut child_page);
                            // insert new parent separator
                            if !(child_page.page_type == IndexType::Leaf) {
                                left_page.insert_key(sep_key);
                            }
                            parent_page.get_children_mut().remove(index);
                            self.buffer_pool.free_page(child_id, FLUSH);
                            underflow_node = Some(parent_id);
                        });
                        continue;
                    }

                    // Merge with right sibling
                    if let Some(right_id) = right_sibling {
                        with_write_pages!(self.buffer_pool, [(right_id, right_page)], FLUSH, {
                            let sep_key = parent_page.keys.remove(index); // remove parent separator
                            // insert new parent separator
                            if !(child_page.page_type == IndexType::Leaf) {
                                child_page.insert_key(sep_key);
                            }
                            child_page.merge(&mut right_page);
                            parent_page.get_children_mut().remove(index + 1);
                            self.buffer_pool.free_page(right_id, FLUSH);
                            underflow_node = Some(parent_id);
                        });
                        continue;
                    }
                });
            }  else {
                // Reached root, collapse if needed
                let root_id = self.root;
                with_read_pages!(self.buffer_pool, [(root_id, root_page)], {
                    if root_page.page_type == IndexType::Internal && root_page.get_children().len() == 1 {
                        let new_root = root_page.get_children()[0];
                        self.root = new_root;
                        self.buffer_pool.free_page(root_id, FLUSH);
                    }
                });
                break;
            }
        }

        true
    }

    /// Find given key in leaf page
    fn descend_to_leaf(&self, key: i64) -> Vec::<PageId> {
        let mut stack: Vec<PageId> = Vec::new();
        let mut curr_id = self.root;

        loop {
            with_read_pages!(self.buffer_pool, [(curr_id, curr_page)], {
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

    /// Debug Helper: Print the B+ tree in a readable form
    pub fn print_tree(&self) {
        println!("B+ Tree (root id: {})", self.root);
        self.print_node(self.root, 0);
    }

    /// Recursive helper to print a node and its children
    fn print_node(&self, page_id: PageId, level: usize) {
        let indent = "  ".repeat(level);

        with_read_pages!(self.buffer_pool, [(page_id, page)], {
            match page.page_type {
                IndexType::Leaf => {
                    println!("{}Leaf[{}] keys: {:?}", indent, page_id, page.keys);
                }
                IndexType::Internal => {
                    println!("{}Internal[{}] keys: {:?}", indent, page_id, page.keys);
                    for &child_id in page.get_children() {
                        self.print_node(child_id, level + 1);
                    }
                }
            }
        });
    }
}
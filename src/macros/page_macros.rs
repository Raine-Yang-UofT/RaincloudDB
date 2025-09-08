use paste::paste;

#[macro_export]
macro_rules! with_create_pages {
    ($tree:expr, [ $( ($id:expr, $var:ident) ),+ ], $flush:expr, $body:block ) => {{
        $(
            paste! {
                // create page (hard failure if it fails)
                let [<frame_$var>] = $tree.buffer_pool.create_page().expect("create failed");
                let mut $var = [<frame_$var>].write().unwrap();
            }
        )+
        // execute body with page binding
        { $body }
        // unpin & flush
        $(
            drop($var);
            $tree.buffer_pool.unpin_page($id, true).expect("unpin failed");
            if $flush {
                $tree.buffer_pool.flush_page($id).expect("flush failed");
            }
        )+
    }};
}

#[macro_export]
macro_rules! with_write_pages {
    ($tree:expr, [ $( ($id:expr, $var:ident) ),+ ], $flush:expr, $body:block ) => {{
        let mut all_pages_fetched = true;
        $(
            paste! {
                let [<frame_$var>] = match $tree.buffer_pool.fetch_page($id) {
                    Ok(f) => Some(f),
                    Err(_) => {
                        all_pages_fetched = false;
                        None
                    }
                };
                let mut $var = None;
                if let Some(ref frame) = [<frame_$var>] {
                    $var = Some(frame.write().unwrap());
                }
            }
        )+

        if all_pages_fetched {
            $(
                let mut $var = $var.unwrap();
            )+
            { $body }

            // unpin & flush
            $(
                drop($var);
                $tree.buffer_pool.unpin_page($id, true).expect("unpin failed");
                if $flush {
                    $tree.buffer_pool.flush_page($id).expect("flush failed");
                }
            )+
        }
    }};
}

#[macro_export]
macro_rules! with_read_pages {
    ($tree:expr, [ $( ($id:expr, $var:ident) ),+ ], $flush:expr, $body:block ) => {{
        let mut all_pages_fetched = true;
        $(
            paste! {
                let [<frame_$var>] = match $tree.buffer_pool.fetch_page($id) {
                    Ok(f) => Some(f),
                    Err(_) => {
                        all_pages_fetched = false;
                        None
                    }
                };
                let mut $var = None;
                if let Some(ref frame) = [<frame_$var>] {
                    $var = Some(frame.read().unwrap());
                }
            }
        )+

        if all_pages_fetched {
            $(
                let $var = $var.unwrap();
            )+
            { $body }
            // unpin & flush
            $(
                drop($var);
                $tree.buffer_pool.unpin_page($id, false).expect("unpin failed");
                if $flush {
                    $tree.buffer_pool.flush_page($id).expect("flush failed");
                }
            )+
        }
    }};
}

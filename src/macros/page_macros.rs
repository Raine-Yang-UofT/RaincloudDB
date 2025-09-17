#[macro_export]
macro_rules! with_create_pages {
    ($pool:expr, [ $( ($id:expr, $var:ident) ),+ ], $flush:expr, $body:block ) => {{
        paste! {
            $(
                let mut [<pg_$var>] = $pool.buffer_pool.create_page().expect("create failed");
                let mut $var = [<pg_$var>].write();
                $id = $var.get_id();
            )+

            { $body }

            // unpin & flush
            $(
                drop($var);    // explicit drop is required to unpin page
                if $flush {
                    $pool.buffer_pool.flush_page($id).expect("flush failed");
                }
            )+
        }
    }};
}

#[macro_export]
macro_rules! with_write_pages {
    ($pool:expr, [ $( ($id:expr, $var:ident) ),+ ], $flush:expr, $body:block ) => {{
        paste! {
            $(
                let mut [<pg_$var>] = $pool.buffer_pool.fetch_page($id).expect("fetch failed");
                let mut $var = [<pg_$var>].write();
            )+

            { $body }

            // unpin & flush
            $(
                drop($var);    // explicit drop is required to unpin page
                if $flush {
                    $pool.buffer_pool.flush_page($id).expect("flush failed");
                }
            )+
        }
    }};
}

#[macro_export]
macro_rules! with_read_pages {
    ($pool:expr, [ $( ($id:expr, $var:ident) ),+ ], $body:block ) => {{
        paste! {
            $(
                let [<pg_$var>] = $pool.buffer_pool.fetch_page($id).expect("fetch failed");
                let $var = [<pg_$var>].read();
            )+

            { $body }
        }
    }};
}

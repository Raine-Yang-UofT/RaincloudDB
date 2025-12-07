#[macro_export]
macro_rules! with_create_pages {
    ($pool:expr, [ $( ($id:expr, $var:ident) ),+ ], $flush:expr, $body:block ) => {{
        paste! {
            $(
                let mut [<pg_$var>] = $pool.create_page().expect("create failed");
                let mut $var = [<pg_$var>].write();
                $id = $var.get_id();
            )+

            { $body }

            // unpin & flush
            $(
                drop($var);    // explicit drop is required to unpin page
                if $flush {
                    $pool.flush_page($id).expect("flush failed");
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
                let mut [<pg_$var>] = $pool.fetch_page($id).expect("fetch failed");
                let mut $var = [<pg_$var>].write();
            )+

            { $body }

            // unpin & flush
            $(
                drop($var);    // explicit drop is required to unpin page
                if $flush {
                    $pool.flush_page($id).expect("flush failed");
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
                let [<pg_$var>] = $pool.fetch_page($id).expect("fetch failed");
                let $var = [<pg_$var>].read();
            )+

            { $body }
        }
    }};
}

#[macro_export]
/// get a bit from a bitmap
macro_rules! bitmap_get {
    ($bitmap:expr, $index:expr) => {{
        let index = $index;
        let byte_index = index / 8;
        let bit_index = index % 8;
        if byte_index >= $bitmap.len() {
            false
        } else {
            ($bitmap[byte_index] >> bit_index) & 1 != 0
        }
    }};
}

#[macro_export]
/// Set a bit in a bitmap
macro_rules! bitmap_set {
    ($bitmap:expr, $index:expr, $value:expr) => {{
        let index = $index;
        let byte_index = index / 8;
        let bit_index = index % 8;
        if $value {
            $bitmap[byte_index] |= 1 << bit_index;
        } else {
            $bitmap[byte_index] &= !(1 << bit_index);
        }
    }};
}


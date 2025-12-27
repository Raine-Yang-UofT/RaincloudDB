use raincloud_db::compiler::ast::{Literal, ColumnDef, DataType, RowDef};

#[test]
fn row_def_serialization_int() {
    let schema = vec![
        ColumnDef{ name: "a".to_string(), data_type: DataType::Int },
        ColumnDef{ name: "b".to_string(), data_type: DataType::Int },
    ];

    let row = RowDef {
        record: vec![Literal::Int(1), Literal::Int(2)],
    };

    let bytes = row.serialize().unwrap();
    let decoded = RowDef::deserialize(&bytes, &schema).unwrap();

    assert_eq!(row, decoded);
}

#[test]
fn row_def_serialization_char() {
    let schema = vec![
        ColumnDef{ name: "a".to_string(), data_type: DataType::Char(4) },
        ColumnDef{ name: "b".to_string(), data_type: DataType::Char(3) },
    ];

    let row = RowDef {
        record: vec![
            Literal::String("ab\0\0".into()),
            Literal::String("xyz".into()),
        ],
    };

    let bytes = row.serialize().unwrap();
    let decoded = RowDef::deserialize(&bytes, &schema).unwrap();

    assert_eq!(
        decoded.record,
        vec![
            Literal::String("ab".into()),
            Literal::String("xyz".into()),
        ]
    );
}

#[test]
fn row_def_serialization_mixed() {
    let schema = vec![
        ColumnDef{ name: "a".to_string(), data_type: DataType::Int },
        ColumnDef{ name: "b".to_string(), data_type: DataType::Char(5) },
        ColumnDef{ name: "c".to_string(), data_type: DataType::Int },
    ];

    let row = RowDef {
        record: vec![
            Literal::Int(10),
            Literal::String("hi\0\0\0".into()),
            Literal::Int(-3),
        ],
    };

    let bytes = row.serialize().unwrap();
    let decoded = RowDef::deserialize(&bytes, &schema).unwrap();

    assert_eq!(decoded.record[0], Literal::Int(10));
    assert_eq!(decoded.record[1], Literal::String("hi".into()));
    assert_eq!(decoded.record[2], Literal::Int(-3));
}


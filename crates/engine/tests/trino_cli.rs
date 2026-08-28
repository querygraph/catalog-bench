use catalog_bench_conformance::sha256_hex;
use catalog_bench_engine::{
    decode_trino_canonical_read, decode_trino_single_text, decode_trino_single_u64, CanonicalRead,
    TrinoCliDecodeError, TrinoCliScalarError,
};

#[test]
fn reconstructs_canonical_arrays_in_oracle_column_order() {
    let canonical = b"[0,\"category-0\",7]\n[1,\"category-1\",107]\n";
    let expected = oracle(2, canonical);
    let output = b"{\"amount_cents\":7,\"id\":0,\"category\":\"category-0\"}\n\
{\"category\":\"category-1\",\"amount_cents\":107,\"id\":1}\n";

    let read = decode_trino_canonical_read(output, &expected).unwrap();
    assert_eq!(read.rows, expected.rows);
    assert_eq!(read.bytes, expected.bytes);
    assert_eq!(read.sha256, expected.sha256);
}

#[test]
fn rejects_duplicate_missing_extra_nested_and_malformed_rows() {
    let expected = oracle(1, b"[0,\"category-0\",7]\n");
    for (output, kind) in [
        (
            b"{\"id\":0,\"id\":1,\"category\":\"category-0\",\"amount_cents\":7}\n".as_slice(),
            TrinoCliDecodeError::DuplicateColumn,
        ),
        (
            b"{\"id\":0,\"category\":\"category-0\"}\n".as_slice(),
            TrinoCliDecodeError::UnexpectedColumns,
        ),
        (
            b"{\"id\":0,\"category\":\"category-0\",\"amount_cents\":7,\"extra\":1}\n".as_slice(),
            TrinoCliDecodeError::UnexpectedColumns,
        ),
        (
            b"{\"id\":0,\"category\":[\"category-0\"],\"amount_cents\":7}\n".as_slice(),
            TrinoCliDecodeError::UnsupportedValue,
        ),
        (
            b"{not-json}\n".as_slice(),
            TrinoCliDecodeError::MalformedRow,
        ),
        (
            b"{\"id\":0,\"category\":\"category-0\",\"amount_cents\":7}\n\n".as_slice(),
            TrinoCliDecodeError::MalformedRow,
        ),
    ] {
        assert_eq!(decode_trino_canonical_read(output, &expected), Err(kind));
    }
}

#[test]
fn enforces_trailing_lf_row_and_total_byte_bounds() {
    let expected = oracle(1, b"[0,\"category-0\",7]\n");
    assert_eq!(
        decode_trino_canonical_read(
            b"{\"id\":0,\"category\":\"category-0\",\"amount_cents\":7}",
            &expected,
        ),
        Err(TrinoCliDecodeError::MissingTrailingLf)
    );
    let two_rows = b"{\"id\":0,\"category\":\"category-0\",\"amount_cents\":7}\n\
{\"id\":1,\"category\":\"category-1\",\"amount_cents\":107}\n";
    assert_eq!(
        decode_trino_canonical_read(two_rows, &expected),
        Err(TrinoCliDecodeError::TooManyRows)
    );
    let oversized = vec![b'x'; 16 * 1024 * 1024 + 1];
    assert_eq!(
        decode_trino_canonical_read(&oversized, &expected),
        Err(TrinoCliDecodeError::OutputTooLarge)
    );
}

#[test]
fn empty_output_is_a_valid_zero_row_observation() {
    let expected = oracle(0, b"");
    let read = decode_trino_canonical_read(b"", &expected).unwrap();
    assert_eq!(read.rows, 0);
    assert_eq!(read.bytes, 0);
    assert_eq!(read.sha256, expected.sha256);
}

#[test]
fn decodes_exact_single_count_and_text_rows() {
    assert_eq!(
        decode_trino_single_u64(b"{\"matches\":1}\n", "matches"),
        Ok(1)
    );
    assert_eq!(
        decode_trino_single_text(
            b"{\"file\":\"s3://warehouse/table/metadata/v1.json\"}\n",
            "file",
        ),
        Ok("s3://warehouse/table/metadata/v1.json".to_owned())
    );
}

#[test]
fn scalar_decoder_rejects_shape_duplicates_types_controls_and_bounds() {
    for (output, expected) in [
        (b"".as_slice(), TrinoCliScalarError::InvalidShape),
        (
            b"{\"matches\":1}".as_slice(),
            TrinoCliScalarError::InvalidShape,
        ),
        (
            b"{\"matches\":1}\n{\"matches\":2}\n".as_slice(),
            TrinoCliScalarError::InvalidShape,
        ),
        (
            b"{\"matches\":1,\"other\":2}\n".as_slice(),
            TrinoCliScalarError::InvalidShape,
        ),
        (
            b"{\"matches\":1,\"matches\":2}\n".as_slice(),
            TrinoCliScalarError::DuplicateColumn,
        ),
        (
            b"{\"matches\":\"1\"}\n".as_slice(),
            TrinoCliScalarError::InvalidValue,
        ),
    ] {
        assert_eq!(decode_trino_single_u64(output, "matches"), Err(expected));
    }
    assert_eq!(
        decode_trino_single_text(b"{\"value\":\"line\\nbreak\"}\n", "value"),
        Err(TrinoCliScalarError::InvalidValue)
    );
    assert_eq!(
        decode_trino_single_u64(b"{\"matches\":1}\n", "bad\ncolumn"),
        Err(TrinoCliScalarError::InvalidShape)
    );
    let oversized = vec![b'x'; 64 * 1024 + 1];
    assert_eq!(
        decode_trino_single_text(&oversized, "value"),
        Err(TrinoCliScalarError::OutputTooLarge)
    );
}

fn oracle(rows: u64, canonical: &[u8]) -> CanonicalRead {
    CanonicalRead {
        rows,
        bytes: canonical.len() as u64,
        sha256: sha256_hex(canonical),
        columns: vec![
            "id".to_owned(),
            "category".to_owned(),
            "amount_cents".to_owned(),
        ],
    }
}

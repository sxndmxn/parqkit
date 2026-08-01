Volatile Parquet fixtures for manual Parqkit testing

These files were generated with /home/runner/work/parqkit/parqkit/src/bin/parqkit-generate.rs.
They are small, random fixtures intended for exercising head, tail, schema, count,
stats, info, convert, and merge behavior with different profiles/compression codecs.

Files
- mixed-snappy.parquet   rows=64  cols=8  seed=918273  profile=mixed      compression=snappy
- sparse-gzip.parquet    rows=96  cols=6  seed=271828  profile=sparse     compression=gzip
- unicode-zstd.parquet   rows=48  cols=4  seed=314159  profile=unicode    compression=zstd
- edge-none.parquet      rows=40  cols=8  seed=161803  profile=edge-cases compression=none

Regenerate with:
- cargo run --bin parqkit-generate -- --rows 64 --cols 8 --seed 918273 --profile mixed --compression snappy --output /home/runner/work/parqkit/parqkit/tests/fixtures/volatile/mixed-snappy.parquet
- cargo run --bin parqkit-generate -- --rows 96 --cols 6 --seed 271828 --profile sparse --compression gzip --output /home/runner/work/parqkit/parqkit/tests/fixtures/volatile/sparse-gzip.parquet
- cargo run --bin parqkit-generate -- --rows 48 --cols 4 --seed 314159 --profile unicode --compression zstd --output /home/runner/work/parqkit/parqkit/tests/fixtures/volatile/unicode-zstd.parquet
- cargo run --bin parqkit-generate -- --rows 40 --cols 8 --seed 161803 --profile edge-cases --compression none --output /home/runner/work/parqkit/parqkit/tests/fixtures/volatile/edge-none.parquet

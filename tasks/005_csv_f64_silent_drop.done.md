# Surface dropped F64 values in CSV loading

When `ValueId::F64(bits).to_f64()` returns None during CSV property
parsing, the property is silently skipped. Handle the None case explicitly
(error or documented skip-with-count) so invalid floats don't vanish.

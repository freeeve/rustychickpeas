//! Utility functions for Python bindings

use pyo3::prelude::*;
use pyo3::types::PyInt;
use pyo3::IntoPyObjectExt;
use rustychickpeas_core::graph_snapshot::Atoms;
use rustychickpeas_core::ValueId;

/// Helper to convert Python value to PropertyValue for GraphSnapshot queries
/// Note: Check bool before int, as True/False can be extracted as int
pub fn py_to_property_value(
    value: &Bound<'_, PyAny>,
) -> PyResult<rustychickpeas_core::PropertyValue> {
    use rustychickpeas_core::PropertyValue;
    // Check bool first, as True/False can be extracted as int
    if let Ok(b) = value.extract::<bool>() {
        Ok(PropertyValue::Boolean(b))
    } else if let Ok(s) = value.extract::<String>() {
        Ok(PropertyValue::String(s))
    } else if value.is_instance_of::<PyInt>() {
        match value.extract::<i64>() {
            Ok(i) => Ok(PropertyValue::Integer(i)),
            Err(_) => Err(PyErr::new::<pyo3::exceptions::PyOverflowError, _>(
                "Integer property value out of range for i64",
            )),
        }
    } else if let Ok(f) = value.extract::<f64>() {
        Ok(PropertyValue::Float(f))
    } else {
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Property value must be str, int, float, or bool",
        ))
    }
}

/// Deterministic 64-bit hash (splitmix64 finalizer) for stable `__hash__` values
///
/// Used instead of `std::collections::hash_map::DefaultHasher`, whose output is
/// not guaranteed to be stable across Rust releases.
pub fn stable_hash_u64(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// O(1) degree of `node` in `direction`, read straight from the resident CSR
/// offsets (no neighbor scan). `Both` sums the two sides.
pub fn csr_degree(
    snapshot: &rustychickpeas_core::GraphSnapshot,
    node: u32,
    direction: rustychickpeas_core::Direction,
) -> usize {
    use rustychickpeas_core::Direction;
    let one = |offsets: &[u32]| {
        let i = node as usize;
        if i + 1 >= offsets.len() {
            0
        } else {
            (offsets[i + 1] - offsets[i]) as usize
        }
    };
    match direction {
        Direction::Outgoing => one(&snapshot.out_offsets),
        Direction::Incoming => one(&snapshot.in_offsets),
        Direction::Both => one(&snapshot.out_offsets) + one(&snapshot.in_offsets),
    }
}

/// Convert a ValueId to a Python object, resolving strings through the Atoms table
pub fn value_id_to_pyobject(py: Python<'_>, vid: ValueId, atoms: &Atoms) -> Option<PyObject> {
    match vid {
        ValueId::Str(sid) => atoms.resolve(sid).and_then(|s| s.into_py_any(py).ok()),
        ValueId::I64(i) => i.into_py_any(py).ok(),
        ValueId::F64(bits) => f64::from_bits(bits).into_py_any(py).ok(),
        ValueId::Bool(b) => b.into_py_any(py).ok(),
    }
}

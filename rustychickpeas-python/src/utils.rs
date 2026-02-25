//! Utility functions for Python bindings

use pyo3::prelude::*;
use pyo3::types::PyBool;
use rustychickpeas_core::ValueId;
use rustychickpeas_core::graph_snapshot::Atoms;

/// Helper to convert Python value to PropertyValue for GraphSnapshot queries
/// Note: Check bool before int, as True/False can be extracted as int
pub fn py_to_property_value(value: &PyAny) -> PyResult<rustychickpeas_core::PropertyValue> {
    use rustychickpeas_core::PropertyValue;
    // Check bool first, as True/False can be extracted as int
    if let Ok(b) = value.extract::<bool>() {
        Ok(PropertyValue::Boolean(b))
    } else if let Ok(s) = value.extract::<String>() {
        Ok(PropertyValue::String(s))
    } else if let Ok(i) = value.extract::<i64>() {
        Ok(PropertyValue::Integer(i))
    } else if let Ok(f) = value.extract::<f64>() {
        Ok(PropertyValue::Float(f))
    } else {
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Property value must be str, int, float, or bool",
        ))
    }
}

/// Convert a ValueId to a Python object, resolving strings through the Atoms table
pub fn value_id_to_pyobject(py: Python, vid: ValueId, atoms: &Atoms) -> Option<PyObject> {
    match vid {
        ValueId::Str(sid) => {
            atoms.resolve(sid).map(|s| s.to_object(py))
        }
        ValueId::I64(i) => Some(i.to_object(py)),
        ValueId::F64(bits) => Some(f64::from_bits(bits).to_object(py)),
        ValueId::Bool(b) => Some(PyBool::new(py, b).into_py(py)),
    }
}


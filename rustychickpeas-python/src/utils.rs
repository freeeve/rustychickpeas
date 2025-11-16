//! Utility functions for Python bindings

use pyo3::prelude::*;

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


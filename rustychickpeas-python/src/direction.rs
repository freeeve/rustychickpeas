//! Direction enum for relationship traversal

use pyo3::prelude::*;

/// Direction enum for relationship traversal
#[pyclass]
#[derive(Clone, Copy)]
pub enum Direction {
    Outgoing,
    Incoming,
    Both,
}

impl From<Direction> for rustychickpeas_core::Direction {
    fn from(dir: Direction) -> Self {
        match dir {
            Direction::Outgoing => rustychickpeas_core::Direction::Outgoing,
            Direction::Incoming => rustychickpeas_core::Direction::Incoming,
            Direction::Both => rustychickpeas_core::Direction::Both,
        }
    }
}


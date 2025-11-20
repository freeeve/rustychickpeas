//! Integration test for Python API coverage
//! 
//! This test runs the Python tests and allows cargo-tarpaulin to track
//! which Rust code paths are executed by the Python API.
//! 
//! To run with coverage:
//! ```bash
//! cargo tarpaulin --test python_api_coverage_test --follow-exec
//! ```

use std::process::Command;
use std::path::PathBuf;

/// Find the project root directory
fn find_project_root() -> PathBuf {
    let mut dir = std::env::current_dir().unwrap();
    loop {
        if dir.join("Cargo.toml").exists() && dir.join("rustychickpeas-python").exists() {
            return dir;
        }
        match dir.parent() {
            Some(parent) => dir = parent.to_path_buf(),
            None => panic!("Could not find project root"),
        }
    }
}

#[test]
#[ignore] // Ignore by default - this is for coverage tracking, not regular testing
fn run_python_tests_for_coverage() {
    let project_root = find_project_root();
    let python_dir = project_root.join("rustychickpeas-python");
    
    // Check if Python extension is built
    if !python_dir.exists() {
        eprintln!("Python package directory not found. Skipping Python API coverage test.");
        return;
    }
    
    // Build the Python extension in debug mode if needed
    // Note: This should be done before running tarpaulin
    let build_status = Command::new("maturin")
        .arg("develop")
        .current_dir(&python_dir)
        .status();
    
    if let Err(e) = build_status {
        eprintln!("Warning: Could not build Python extension: {}", e);
        eprintln!("Make sure maturin is installed and the extension is built.");
        return;
    }
    
    // Find the Python executable from the venv if it exists
    let python_exe = if python_dir.join(".venv").exists() {
        if cfg!(target_os = "windows") {
            python_dir.join(".venv").join("Scripts").join("python.exe")
        } else {
            python_dir.join(".venv").join("bin").join("python")
        }
    } else {
        // Fall back to system python
        PathBuf::from("python")
    };
    
    // Run pytest using the venv Python if available
    let output = Command::new(&python_exe)
        .arg("-m")
        .arg("pytest")
        .arg("tests/")
        .arg("-v")
        .current_dir(&python_dir)
        .output();
    
    match output {
        Ok(result) => {
            // Print stdout and stderr for debugging
            if !result.stdout.is_empty() {
                println!("{}", String::from_utf8_lossy(&result.stdout));
            }
            if !result.stderr.is_empty() {
                eprintln!("{}", String::from_utf8_lossy(&result.stderr));
            }
            
            // Don't fail the test if pytest fails - we just want coverage data
            // The actual test results are less important than tracking coverage
            if !result.status.success() {
                eprintln!("Python tests completed with exit code: {:?}", result.status.code());
            }
        }
        Err(e) => {
            eprintln!("Failed to run Python tests: {}", e);
            eprintln!("Make sure pytest is installed: pip install pytest pyarrow");
        }
    }
}


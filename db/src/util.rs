use libc::{self, c_char, c_void};
use std::ffi::{CStr, CString};

pub fn error_message(ptr: *const c_char) -> String {
    let cstr = unsafe { CStr::from_ptr(ptr as *const _) };
    let s = String::from_utf8_lossy(cstr.to_bytes()).into_owned();
    unsafe {
        libc::free(ptr as *mut c_void);
    }
    s
}

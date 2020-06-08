// Copyright 2020 The Chubao Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
pub mod coding;
pub mod config;
pub mod entity;
pub mod error;
pub mod http_client;
pub mod net;
pub mod time;

#[macro_export]
macro_rules! sleep {
    ($x:expr) => {{
        std::thread::sleep(std::time::Duration::from_millis($x))
    }};
}

//println stack trace
pub fn stack_trace() {
    use log::Level::Debug;
    use log::{debug, log_enabled};

    if !log_enabled!(Debug) {
        return;
    }

    backtrace::trace(|frame| {
        // Resolve this instruction pointer to a symbol name
        backtrace::resolve_frame(frame, |symbol| {
            let line = format!(
                "ERROR========= name:{:?} line:{:?}",
                symbol.name(),
                symbol.lineno().unwrap_or(0)
            );

            if line.contains("chubaodb::")
                && !line.contains("chubaodb::util::stack_trace")
                && !line.contains("chubaodb::util::error::")
            {
                debug!("{}", line);
            }
        });

        true // keep going to the next frame
    });
}

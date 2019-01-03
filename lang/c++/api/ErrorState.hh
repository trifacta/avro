/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef avro_ErrorState_hh__
#define avro_ErrorState_hh__

#include <string>
#include <queue>
#include <iostream>
#include <boost/iostreams/device/null.hpp>
#include <boost/iostreams/stream.hpp>

namespace avro {

struct error_state {
    std::queue<std::string> error_state_messages;
    bool has_errored;
    void recordError(std::string msg) {
        has_errored = true;
        error_state_messages.push(msg);
    }
    void emptyState() {
        std::ostream nullout(nullptr);
        throwError(nullout);
    }
    void throwError() {
        throwError(std::cerr);
    }
    void throwError(std::ostream& output) {
        while (!error_state_messages.empty()) {
            output << error_state_messages.front() << "\n";
            error_state_messages.pop();
        }
        has_errored = false;
    }
} ;

extern error_state avro_error_state;

} // namespace avro

#endif

# Copyright (c) 2026, Alibaba Group Holding Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

foreach(required TEST_BINARY TEST_ROOT)
  if(NOT DEFINED ${required})
    message(FATAL_ERROR "Missing ${required}")
  endif()
endforeach()
file(MAKE_DIRECTORY "${TEST_ROOT}")
string(RANDOM LENGTH 16 ALPHABET 0123456789abcdef test_id)
set(warehouse "${TEST_ROOT}/warehouse-${test_id}")
execute_process(COMMAND "${TEST_BINARY}" "[paimon]" --list-tests
  RESULT_VARIABLE inventory_result OUTPUT_VARIABLE inventory_output ERROR_VARIABLE inventory_error TIMEOUT 30)
message("${inventory_output}${inventory_error}")
# Catch v2 returns the number of listed cases, not zero, for --list-tests.
if(NOT "${inventory_result}" MATCHES "^[0-9]+$" OR inventory_result LESS 1)
  message(FATAL_ERROR "Paimon test inventory failed or was empty (${inventory_result})")
endif()
# DuckDB's bundled Catch prints a TSV header followed by one line per case.
string(REGEX MATCHALL "[^\n]+" inventory_lines "${inventory_output}")
list(LENGTH inventory_lines inventory_line_count)
math(EXPR expected_line_count "${inventory_result} + 1")
if(NOT inventory_output MATCHES "^name\tgroup\n" OR
   NOT inventory_line_count EQUAL expected_line_count)
  message(FATAL_ERROR "Incomplete Paimon test inventory (${inventory_result})")
endif()
execute_process(COMMAND "${CMAKE_COMMAND}" -E env "PAIMON_TEST_ROOT=${warehouse}"
  "${TEST_BINARY}" "[paimon]" --warn NoTests --reporter compact
  RESULT_VARIABLE result TIMEOUT 180)
if(NOT "${result}" STREQUAL "0")
  message(FATAL_ERROR "Snapshot binding test failed (${result}); inputs preserved at ${warehouse}")
endif()
file(REMOVE_RECURSE "${warehouse}")

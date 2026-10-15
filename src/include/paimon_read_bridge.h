/*
 * Copyright (c) 2026, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <stdint.h>

#define PAIMON_READ_BRIDGE_MAGIC   UINT64_C(0x5041494d4f4e5244)
#define PAIMON_READ_BRIDGE_VERSION 1

enum PaimonReadStatus {
	PAIMON_READ_OK = 0,
	PAIMON_READ_INVALID_ARGUMENT = 1,
	PAIMON_READ_UNSUPPORTED = 2,
	PAIMON_READ_OUT_OF_MEMORY = 3,
	PAIMON_READ_INTERNAL_ERROR = 4
};

struct PaimonReadString {
	const char *data;
	uint64_t size;
};

struct PaimonReadOption {
	struct PaimonReadString key;
	struct PaimonReadString value;
};

/* All views remain valid until their descriptor is released. Never free them. */
struct PaimonBoundReadViewV1 {
	struct PaimonReadString warehouse;
	struct PaimonReadString database;
	struct PaimonReadString table;
	struct PaimonReadString location;
	struct PaimonReadString branch;
	/* Native schema JSON includes stable field IDs and bound field types. */
	struct PaimonReadString schema_json;
	int64_t schema_id;
	int64_t snapshot_id;
	/* Zero means empty at bind time, NOT "resolve latest". */
	uint32_t has_snapshot;
	uint64_t option_count;
	const struct PaimonReadOption *options;
};

/* Descriptor-only interface. V1 does not authorize planning, reading or replay. */
struct PaimonReadBridgeV1 {
	uint64_t magic;
	uint32_t version;
	uint32_t struct_size;
	/* bind_data must be NULL or a live DuckDB FunctionData on the matching host. */
	int32_t (*accepts_bind)(const void *bind_data);
	/* On failure, *descriptor is NULL. Diagnostics use a caller-owned buffer. */
	int32_t (*retain)(const void *bind_data, void **descriptor, char *error, uint64_t error_capacity);
	/* Only a live descriptor from this provider may be passed to describe/release. */
	int32_t (*describe)(const void *descriptor, struct PaimonBoundReadViewV1 *view, uint64_t view_size);
	/* Release once; NULL is permitted. No concurrent release and describe. */
	void (*release)(void *descriptor);
};

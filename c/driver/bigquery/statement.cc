// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "statement.h"

#include <cinttypes>
#include <memory>

#include <adbc.h>
#include <arrow/c/bridge.h>
#include <google/cloud/bigquery/storage/v1/bigquery_read_client.h>
#include <google/cloud/bigquery/v2/minimal/internal/job_client.h>
#include <google/cloud/bigquery/v2/minimal/internal/job_options.h>
#include <google/cloud/bigquery/v2/minimal/internal/job_request.h>
#include <google/cloud/bigquery/v2/minimal/internal/job_response.h>
#include <nanoarrow/nanoarrow.hpp>
#include <nlohmann/json.hpp>

#include "common/options.h"
#include "common/utils.h"
#include "connection.h"
#include "database.h"
#include "readrowsiterator.h"

namespace adbc_bigquery {
AdbcStatusCode BigqueryStatement::Bind(struct ArrowArray* values,
                                       struct ArrowSchema* schema,
                                       struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::Bind(struct ArrowArrayStream* stream,
                                       struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::Cancel(struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::ExecuteQuery(struct ::ArrowArrayStream* stream,
                                               int64_t* rows_affected,
                                               struct AdbcError* error) {
  if (stream) {
    ::google::cloud::bigquery_v2_minimal_internal::PostQueryRequest post_query_request;
    post_query_request.set_project_id(connection_->database_->project_id());

    ::google::cloud::bigquery_v2_minimal_internal::QueryRequest query_request;
    if (auto query = GetQueryRequestOption("query"); query) {
      query_request.set_query(options_["query"]);
    } else {
      SetError(error, "[bigquery] Missing SQL query");
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    ::google::cloud::bigquery_v2_minimal_internal::DatasetReference dataset_reference;
    dataset_reference.dataset_id = "google_trends";
    dataset_reference.project_id = connection_->database_->project_id();
    query_request.set_default_dataset(dataset_reference);
    query_request.set_maximum_bytes_billed(1048576000);

    if (auto kind = GetQueryRequestOption("kind"); kind) {
      query_request.set_kind(*kind);
    }
    if (auto parameter_mode = GetQueryRequestOption("parameter_mode"); parameter_mode) {
      query_request.set_parameter_mode(*parameter_mode);
    }
    if (auto location = GetQueryRequestOption("location"); location) {
      query_request.set_location(*location);
    }
    if (auto request_id = GetQueryRequestOption("request_id"); request_id) {
      query_request.set_request_id(*request_id);
    }
    if (auto dry_run = GetQueryRequestOption<bool>("dry_run"); dry_run) {
      query_request.set_dry_run(*dry_run);
    }
    if (auto preserve_nulls = GetQueryRequestOption<bool>("preserve_nulls");
        preserve_nulls) {
      query_request.set_preserve_nulls(*preserve_nulls);
    }
    if (auto use_query_cache = GetQueryRequestOption<bool>("use_query_cache");
        use_query_cache) {
      query_request.set_use_query_cache(*use_query_cache);
    }
    if (auto use_legacy_sql = GetQueryRequestOption<bool>("use_legacy_sql");
        use_legacy_sql) {
      query_request.set_use_legacy_sql(*use_legacy_sql);
    }
    if (auto create_session = GetQueryRequestOption<bool>("create_session");
        create_session) {
      query_request.set_create_session(*create_session);
    }
    if (auto max_results = GetQueryRequestOption<std::uint32_t>("max_results");
        max_results) {
      query_request.set_max_results(*max_results);
    }
    if (auto max_bytes_billed =
            GetQueryRequestOption<std::int64_t>("maximum_bytes_billed");
        max_bytes_billed) {
      query_request.set_maximum_bytes_billed(*max_bytes_billed);
    } else {
      query_request.set_maximum_bytes_billed(104857600);
    }

    post_query_request.set_query_request(std::move(query_request));
    ::google::cloud::bigquery_v2_minimal_internal::JobClient job_client(
        ::google::cloud::bigquery_v2_minimal_internal::MakeBigQueryJobConnection());
    auto status = job_client.Query(post_query_request);
    if (!status.ok()) {
      SetError(error, "%s%" PRId32 ", %s",
               "[bigquery] Cannot execute query: code=", status.status().code(),
               status.status().message().c_str());
      return ADBC_STATUS_INVALID_STATE;
    }
    auto post_query_results = status.value();
    nlohmann::json json;
    ::google::cloud::bigquery_v2_minimal_internal::to_json(json, post_query_results);
    printf("[debug] post_query_results: %s\r\n", json.dump().c_str());

    auto iterator = std::make_shared<ReadRowsIterator>();
    int ret = iterator->init(error);
    if (ret != ADBC_STATUS_OK) {
      return ret;
    }

    stream->private_data = new std::shared_ptr<ReadRowsIterator>(iterator);
    stream->get_next = ReadRowsIterator::get_next;
    stream->get_schema = ReadRowsIterator::get_schema;
    stream->release = ReadRowsIterator::release;

    if (rows_affected) {
      *rows_affected = -1;
    }
  }
  return ADBC_STATUS_OK;
}

AdbcStatusCode BigqueryStatement::ExecuteSchema(struct ArrowSchema* schema,
                                                struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::GetOption(const char* key, char* value, size_t* length,
                                            struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::GetOptionBytes(const char* key, uint8_t* value,
                                                 size_t* length,
                                                 struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::GetOptionDouble(const char* key, double* value,
                                                  struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::GetOptionInt(const char* key, int64_t* value,
                                               struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::GetParameterSchema(struct ArrowSchema* schema,
                                                     struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::New(struct AdbcConnection* connection,
                                      struct AdbcError* error) {
  if (!connection || !connection->private_data) {
    SetError(error, "[bigquery] Must provide an initialized AdbcConnection");
    return ADBC_STATUS_INVALID_ARGUMENT;
  }
  connection_ =
      *reinterpret_cast<std::shared_ptr<BigqueryConnection>*>(connection->private_data);
  return ADBC_STATUS_OK;
}

AdbcStatusCode BigqueryStatement::Prepare(struct AdbcError* error) {
  return ADBC_STATUS_OK;
}

AdbcStatusCode BigqueryStatement::Release(struct AdbcError* error) {
  return ADBC_STATUS_OK;
}

AdbcStatusCode BigqueryStatement::SetOption(const char* key, const char* value,
                                            struct AdbcError* error) {
  options_[key] = value;
  return ADBC_STATUS_OK;
}

AdbcStatusCode BigqueryStatement::SetOptionBytes(const char* key, const uint8_t* value,
                                                 size_t length, struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::SetOptionDouble(const char* key, double value,
                                                  struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::SetOptionInt(const char* key, int64_t value,
                                               struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::SetSqlQuery(const char* query,
                                              struct AdbcError* error) {
  options_["query"] = query;
  return ADBC_STATUS_OK;
}

}  // namespace adbc_bigquery

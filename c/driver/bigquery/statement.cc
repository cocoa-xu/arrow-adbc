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
    auto job_configuration_query =
        ::google::cloud::bigquery_v2_minimal_internal::JobConfigurationQuery();
    if (auto query = GetQueryRequestOption("query"); query) {
      job_configuration_query.query = *query;
    } else {
      SetError(error, "[bigquery] Missing SQL query");
      return ADBC_STATUS_INVALID_ARGUMENT;
    }

    if (auto parameter_mode = GetQueryRequestOption("parameter_mode"); parameter_mode) {
      job_configuration_query.parameter_mode = *parameter_mode;
    }
    if (auto preserve_nulls = GetQueryRequestOption<bool>("preserve_nulls");
        preserve_nulls) {
      job_configuration_query.preserve_nulls = *preserve_nulls;
    }
    if (auto use_query_cache = GetQueryRequestOption<bool>("use_query_cache");
        use_query_cache) {
      job_configuration_query.use_query_cache = *use_query_cache;
    }
    if (auto use_legacy_sql = GetQueryRequestOption<bool>("use_legacy_sql");
        use_legacy_sql) {
      job_configuration_query.use_legacy_sql = *use_legacy_sql;
    }
    if (auto create_session = GetQueryRequestOption<bool>("create_session");
        create_session) {
      job_configuration_query.create_session = *create_session;
    }
    if (auto max_bytes_billed =
            GetQueryRequestOption<std::int64_t>("maximum_bytes_billed");
        max_bytes_billed) {
      job_configuration_query.maximum_bytes_billed = *max_bytes_billed;
    } else {
      job_configuration_query.maximum_bytes_billed = 104857600;
    }

    ::google::cloud::bigquery_v2_minimal_internal::DatasetReference dataset_reference;
    if (auto default_dataset = GetQueryRequestOption("default_dataset");
        default_dataset) {
      std::string default_dataset_str = *default_dataset;

      dataset_reference.project_id = connection_->database_->project_id();
      dataset_reference.dataset_id = "google_trends";
      job_configuration_query.default_dataset = dataset_reference;
    }
    if (auto destination_table = GetQueryRequestOption("destination_table");
        destination_table) {
      std::string destination_table_str = *destination_table;
      ::google::cloud::bigquery_v2_minimal_internal::TableReference table_reference;
      table_reference.project_id = connection_->database_->project_id();
      table_reference.dataset_id = "google_trends";
      table_reference.table_id = "mytesttable";
      job_configuration_query.destination_table = table_reference;
    }
    dataset_reference.dataset_id = "google_trends";
    dataset_reference.project_id = connection_->database_->project_id();

    ::google::cloud::bigquery_v2_minimal_internal::TableReference table_reference;
    table_reference.project_id = connection_->database_->project_id();
    table_reference.dataset_id = "google_trends";
    table_reference.table_id = "mytesttable";
    job_configuration_query.destination_table = table_reference;
    job_configuration_query.script_options.key_result_statement =
        ::google::cloud::bigquery_v2_minimal_internal::KeyResultStatementKind::Last();

    ::google::cloud::bigquery_v2_minimal_internal::JobConfiguration job_configuration;
    job_configuration.query = job_configuration_query;

    ::google::cloud::bigquery_v2_minimal_internal::Job job;
    job.configuration = job_configuration;
    job.job_reference.location = "US";
    job.configuration.query.allow_large_results = true;
    job.statistics.script_statistics.evaluation_kind =
        ::google::cloud::bigquery_v2_minimal_internal::EvaluationKind::Statement();
    job.statistics.job_query_stats.search_statistics.index_usage_mode =
        ::google::cloud::bigquery_v2_minimal_internal::IndexUsageMode::FullyUsed();

    ::google::cloud::bigquery_v2_minimal_internal::InsertJobRequest insert_job_request;
    insert_job_request.set_project_id(connection_->database_->project_id());
    insert_job_request.set_job(job);

    ::google::cloud::bigquery_v2_minimal_internal::JobClient job_client(
        ::google::cloud::bigquery_v2_minimal_internal::MakeBigQueryJobConnection());
    auto status = job_client.InsertJob(insert_job_request);

    if (!status.ok()) {
      SetError(error, "%s%" PRId32 ", %s",
               "[bigquery] Cannot execute query: code=", status.status().code(),
               status.status().message().c_str());
      return ADBC_STATUS_INVALID_STATE;
    }

    std::string project_name =
        "projects/" + job_configuration_query.destination_table.project_id;
    std::string table_name = project_name + "/datasets" +
                             job_configuration_query.destination_table.dataset_id +
                             "/tables/" +
                             job_configuration_query.destination_table.table_id;
    auto iterator = std::make_shared<ReadRowsIterator>(project_name, table_name);
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

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
#include <google/cloud/bigquery/storage/v1/bigquery_read_client.h>
#include <nanoarrow/nanoarrow.hpp>

#include "common/options.h"
#include "common/utils.h"
#include "connection.h"
#include "database.h"

namespace adbc_bigquery {

AdbcStatusCode ReadRowsIterator::init(struct AdbcError* error) {
  connection_ = ::google::cloud::bigquery_storage_v1::MakeBigQueryReadConnection();
  client_ = std::make_shared<::google::cloud::bigquery_storage_v1::BigQueryReadClient>(connection_);
  session_ = std::make_shared<::google::cloud::bigquery::storage::v1::ReadSession>();
  session_->set_data_format(::google::cloud::bigquery::storage::v1::DataFormat::ARROW);
  session_->set_table(table_name_);

  constexpr std::int32_t kMaxReadStreams = 1;
  auto session = client_->CreateReadSession(project_name_, *session_, kMaxReadStreams);
  if (!session) {
    auto& status = session.status();
    SetError(error, "%s%" PRId32 ", %s", "[bigquery] Cannot create read session: code=", status.code(), status.message().c_str());
    return ADBC_STATUS_INVALID_STATE;
  }

  auto response = client_->ReadRows(session->streams(0).name(), 0);
  response_ = std::make_shared<ReadRowsResponse>(std::move(response));

  return ADBC_STATUS_OK;
}

int ReadRowsIterator::get_next(struct ArrowArrayStream* stream, struct ArrowArray* out) {
  if (!stream || !out) {
    return ADBC_STATUS_INVALID_ARGUMENT;
  }

  auto* ptr = reinterpret_cast<std::shared_ptr<ReadRowsIterator>*>(stream->private_data);
  if (!ptr) {
    return ADBC_STATUS_INVALID_STATE;
  }

  std::shared_ptr<ReadRowsIterator> &iterator = *ptr;
  auto cur = iterator->response_->begin();
  if (cur == iterator->response_->end()) {
    return 0;
  }

  auto& row = *cur;
  if (!row.ok()) {
    return ADBC_STATUS_INTERNAL;
  }

  auto& record_batch = row->arrow_record_batch();
  // TODO_BIGQUERY: parse record batch
  return 0;
}

int ReadRowsIterator::get_schema(struct ArrowArrayStream* stream, struct ArrowSchema* out) {
  if (!stream || !out) {
    return ADBC_STATUS_INVALID_ARGUMENT;
  }

  auto* ptr = reinterpret_cast<std::shared_ptr<ReadRowsIterator>*>(stream->private_data);
  if (!ptr) {
    return ADBC_STATUS_INVALID_STATE;
  }

  std::shared_ptr<ReadRowsIterator> &iterator = *ptr;
  auto& session = iterator->session_;
  auto& schema = session->arrow_schema();

  // TODO_BIGQUERY: parse schema
  return 0;
}

void ReadRowsIterator::release(struct ArrowArrayStream* stream) {
  if (stream && stream->private_data) {
    auto* ptr = reinterpret_cast<std::shared_ptr<ReadRowsIterator>*>(stream->private_data);
    if (ptr) {
      delete ptr;
    }
    stream->private_data = nullptr;
  }
}

AdbcStatusCode BigqueryStatement::Bind(struct ArrowArray* values, struct ArrowSchema* schema,
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
                                               int64_t* rows_affected, struct AdbcError* error) {
  if (stream) {
    auto iterator = std::make_shared<ReadRowsIterator>(
      connection_->database_->project_name_,
      connection_->database_->table_name_);
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

AdbcStatusCode BigqueryStatement::GetOptionBytes(const char* key, uint8_t* value, size_t* length,
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
    SetError(error, "%s", "[bigquery] Must provide an initialized AdbcConnection");
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
  return ADBC_STATUS_NOT_IMPLEMENTED;
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

AdbcStatusCode BigqueryStatement::SetSqlQuery(const char* query, struct AdbcError* error) {
  return ADBC_STATUS_OK;
}

}  // namespace adbc_bigquery

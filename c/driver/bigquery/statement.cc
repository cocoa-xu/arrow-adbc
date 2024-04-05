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
#include <flatbuffers/flatbuffers.h>
#include <arrow/Schema_generated.h>
#include <arrow/Message_generated.h>

#include "common/options.h"
#include "common/utils.h"
#include "connection.h"
#include "database.h"

namespace adbc_bigquery {

int parse_encapsulated_message(const std::string& data, org::apache::arrow::flatbuf::MessageHeader expected_header, void * out_data, void * private_data) {
  // https://arrow.apache.org/docs/format/Columnar.html#encapsulated-message-format
  if (data.length() == 0) return ADBC_STATUS_OK;

  uintptr_t * data_ptr = (uintptr_t *)data.data();
  bool continuation = *(uint32_t *)data_ptr == 0xFFFFFFFF;
  data_ptr = (uintptr_t *)(((uint64_t)(uint64_t *)data_ptr) + 4);

  // metadata_size:
  // A 32-bit little-endian length prefix indicating the metadata size
  int32_t metadata_size = *(int32_t *)data_ptr;
#if ADBC_DRIVER_BIGQUERY_ENDIAN == 0
  metadata_size = __builtin_bswap32(metadata_size);
#endif
  data_ptr = (uintptr_t *)(((uint64_t)(uint64_t *)data_ptr) + 4);
  printf("  continuation: %d\r\n", continuation);
  printf("  metadata_size: %d\r\n", metadata_size);
  auto header = org::apache::arrow::flatbuf::GetMessage(data_ptr);
  auto body_data = (uintptr_t *)(((uint64_t)(uint64_t *)data_ptr) + metadata_size);
  printf("body_data = %p\r\n", body_data);
  auto header_type = header->header_type();
  printf("  header_type: %hhu\r\n", header_type);
  if (header_type == expected_header) {
      if (header_type == org::apache::arrow::flatbuf::MessageHeader::Schema) {
          std::cout << "  Schema\r\n";
          auto schema = header->header_as_Schema();
          auto fields = schema->fields();
          for (size_t i = 0; i < fields->size(); i++) {
              auto field = fields->Get(i);
              std::cout << "    name=" << field->name()->str() << ", ";
              std::cout << "type=" << org::apache::arrow::flatbuf::EnumNameType(field->type_type()) << "\r\n";
          }

          // https://arrow.apache.org/docs/format/CDataInterface.html#data-type-description-format-strings
          struct ArrowSchema * out = (struct ArrowSchema *)out_data;
          ArrowSchemaInit(out);
          out->name = nullptr;
          ArrowSchemaSetTypeStruct(out, fields->size());

          const org::apache::arrow::flatbuf::Int * field_int = nullptr;
          const org::apache::arrow::flatbuf::FloatingPoint * field_fp = nullptr;
          const org::apache::arrow::flatbuf::Decimal * field_decimal = nullptr;
          const org::apache::arrow::flatbuf::Date * field_date = nullptr;
          const org::apache::arrow::flatbuf::Time * field_time = nullptr;
          const org::apache::arrow::flatbuf::Interval * field_interval = nullptr;
          // const org::apache::arrow::flatbuf::Struct_ * field_struct = nullptr;
          const org::apache::arrow::flatbuf::Union * field_union = nullptr;
          for (size_t i = 0; i < fields->size(); i++) {
              auto field = fields->Get(i);
              auto child = out->children[i];
              ArrowSchemaSetName(child, field->name()->str().c_str());
              switch (field->type_type())
              {
              case org::apache::arrow::flatbuf::Type::NONE:
                  ArrowSchemaSetType(child, NANOARROW_TYPE_UNINITIALIZED);
                  break;
              case org::apache::arrow::flatbuf::Type::Null:
                  ArrowSchemaSetType(child, NANOARROW_TYPE_NA);
                  break;
              case org::apache::arrow::flatbuf::Type::Int:
                  field_int = field->type_as_Int();
                  if(field_int->is_signed()) {
                    if (field_int->bitWidth() == 8) {
                      ArrowSchemaSetType(child, NANOARROW_TYPE_INT8);
                    } else if (field_int->bitWidth() == 16) {
                      ArrowSchemaSetType(child, NANOARROW_TYPE_INT16);
                    } else if (field_int->bitWidth() == 32) {
                      ArrowSchemaSetType(child, NANOARROW_TYPE_INT32);
                    } else {
                      ArrowSchemaSetType(child, NANOARROW_TYPE_INT64);
                    }
                  } else {
                    if (field_int->bitWidth() == 8) {
                      ArrowSchemaSetType(child, NANOARROW_TYPE_UINT8);
                    } else if (field_int->bitWidth() == 16) {
                      ArrowSchemaSetType(child, NANOARROW_TYPE_UINT16);
                    } else if (field_int->bitWidth() == 32) {
                      ArrowSchemaSetType(child, NANOARROW_TYPE_UINT32);
                    } else {
                      ArrowSchemaSetType(child, NANOARROW_TYPE_UINT64);
                    }
                  }
                  break;
              case org::apache::arrow::flatbuf::Type::FloatingPoint:
                  field_fp = field->type_as_FloatingPoint();
                  if (field_fp->precision() == org::apache::arrow::flatbuf::Precision::HALF) {
                    ArrowSchemaSetType(child, NANOARROW_TYPE_HALF_FLOAT);
                  } else if (field_fp->precision() == org::apache::arrow::flatbuf::Precision::SINGLE) {
                    ArrowSchemaSetType(child, NANOARROW_TYPE_FLOAT);
                  } else {
                    ArrowSchemaSetType(child, NANOARROW_TYPE_DOUBLE);
                  }
                  break;
              case org::apache::arrow::flatbuf::Type::Binary:
                  ArrowSchemaSetType(child, NANOARROW_TYPE_BINARY);
                  break;
              case org::apache::arrow::flatbuf::Type::Utf8:
                  ArrowSchemaSetType(child, NANOARROW_TYPE_STRING);
                  break;
              case org::apache::arrow::flatbuf::Type::Bool:
                  ArrowSchemaSetType(child, NANOARROW_TYPE_BOOL);
                  break;
              case org::apache::arrow::flatbuf::Type::Decimal:
                  field_decimal = field->type_as_Decimal();
                  char format[32] = {0};
                  int f_len = 0;
                  if (field_decimal->bitWidth() == 128) {
                    f_len = snprintf(format, sizeof(format) - 1, "d:%d,%d", field_decimal->precision(), field_decimal->scale());
                  } else {
                    f_len = snprintf(format, sizeof(format) - 1, "d:%d,%d,%d", field_decimal->precision(), field_decimal->scale(), field_decimal->bitWidth());
                  }
                  // TODO_BIGQUERY: free child->format in release function
                  child->format = (char *)strndup(format, f_len);
                  break;
              case org::apache::arrow::flatbuf::Type::Date:
                  field_date = field->type_as_Date();
                  if (field_date->unit() == org::apache::arrow::flatbuf::DateUnit::DAY) {
                    ArrowSchemaSetType(child, NANOARROW_TYPE_DATE32);
                  } else {
                    ArrowSchemaSetType(child, NANOARROW_TYPE_DATE64);
                  }
                  break;
              case org::apache::arrow::flatbuf::Type::Time:
                  field_time = field->type_as_Time();
                  if (field_time->bitWidth() == 32) {
                    if (field_time->unit() == org::apache::arrow::flatbuf::TimeUnit::SECOND) {
                      child->format = "tts";
                    } else if (field_time->unit() == org::apache::arrow::flatbuf::TimeUnit::MILLISECOND) {
                      child->format = "ttm";
                    }
                  } else if (field_time->bitWidth() == 64) {
                    if (field_time->unit() == org::apache::arrow::flatbuf::TimeUnit::MICROSECOND) {
                      child->format = "ttu";
                    } else if (field_time->unit() == org::apache::arrow::flatbuf::TimeUnit::NANOSECOND) {
                      child->format = "ttn";
                    }
                  }
                  break;
              case org::apache::arrow::flatbuf::Type::Timestamp:
                  ArrowSchemaSetType(child, NANOARROW_TYPE_TIMESTAMP);
                  break;
              case org::apache::arrow::flatbuf::Type::Interval:
                  field_interval = field->type_as_Interval();
                  if (field_interval->unit() == org::apache::arrow::flatbuf::IntervalUnit::YEAR_MONTH) {
                    ArrowSchemaSetType(child, NANOARROW_TYPE_INTERVAL_MONTHS);
                  } else if (field_interval->unit() == org::apache::arrow::flatbuf::IntervalUnit::DAY_TIME) {
                    ArrowSchemaSetType(child, NANOARROW_TYPE_INTERVAL_DAY_TIME);
                  } else {
                    ArrowSchemaSetType(child, NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO);
                  }
                  break;
              case org::apache::arrow::flatbuf::Type::List:
                  ArrowSchemaSetType(child, NANOARROW_TYPE_LIST);
                  break;
              case org::apache::arrow::flatbuf::Type::Struct_:
                  // TODO_BIGQUERY: recursively parse struct?
                  // field_struct = field->type_as_Struct_();
                  ArrowSchemaSetType(child, NANOARROW_TYPE_STRUCT);
                  break;
              case org::apache::arrow::flatbuf::Type::Union:
                  field_union = field->type_as_Union();
                  if (field_union->mode() == org::apache::arrow::flatbuf::UnionMode::Sparse) {
                    ArrowSchemaSetType(child, NANOARROW_TYPE_SPARSE_UNION);
                  } else {
                    ArrowSchemaSetType(child, NANOARROW_TYPE_DENSE_UNION);
                  }
                  break;
              case org::apache::arrow::flatbuf::Type::FixedSizeBinary:
                  ArrowSchemaSetType(child, NANOARROW_TYPE_FIXED_SIZE_BINARY);
                  break;
              case org::apache::arrow::flatbuf::Type::FixedSizeList:
                  ArrowSchemaSetType(child, NANOARROW_TYPE_FIXED_SIZE_LIST);
                  break;
              case org::apache::arrow::flatbuf::Type::Map:
                  ArrowSchemaSetType(child, NANOARROW_TYPE_MAP);
                  break;
              case org::apache::arrow::flatbuf::Type::Duration:
                  ArrowSchemaSetType(child, NANOARROW_TYPE_DURATION);
                  break;
              case org::apache::arrow::flatbuf::Type::LargeBinary:
                  ArrowSchemaSetType(child, NANOARROW_TYPE_LARGE_BINARY);
                  break;
              case org::apache::arrow::flatbuf::Type::LargeUtf8:
                  ArrowSchemaSetType(child, NANOARROW_TYPE_LARGE_STRING);
                  break;
              case org::apache::arrow::flatbuf::Type::LargeList:
                  ArrowSchemaSetType(child, NANOARROW_TYPE_LARGE_LIST);
                  break;
              case org::apache::arrow::flatbuf::Type::RunEndEncoded:
                  child->format = "+r";
                  break;
              case org::apache::arrow::flatbuf::Type::BinaryView:
                  child->format = "vz";
                  break;
              case org::apache::arrow::flatbuf::Type::Utf8View:
                  child->format = "vu";
                  break;
              case org::apache::arrow::flatbuf::Type::ListView:
                  child->format = "+vl";
                  break;
              case org::apache::arrow::flatbuf::Type::LargeListView:
                  child->format = "+vL";
                  break;
              default:
                  // malformed schema?
                  break;
              }
          }
          return ADBC_STATUS_OK;
      } else if (header_type == org::apache::arrow::flatbuf::MessageHeader::RecordBatch) {
          std::cout << "record batch\r\n";
          struct ArrowSchema * schema = (struct ArrowSchema *)private_data;
          auto data_header = header->header_as_RecordBatch();
          auto nodes = data_header->nodes();

          auto buffers = data_header->buffers();
          printf("  buffers: length=%d\r\n", buffers->size());
          int buffer_index = 0;

          struct ArrowArray * out = (struct ArrowArray *)out_data;
          memset(out, 0, sizeof(struct ArrowArray));

          out->n_children = nodes->size();
          out->children = (struct ArrowArray **)malloc(sizeof(struct ArrowArray *) * out->n_children);
          for (size_t i = 0; i < nodes->size(); i++) {
              out->children[i] = (struct ArrowArray *)malloc(sizeof(struct ArrowArray));
              memset(out->children[i], 0, sizeof(struct ArrowArray));
          }

          for (size_t i = 0; i < nodes->size(); i++) {
              auto node = nodes->Get(i);
              auto child_schema = schema->children[i];
              
              // ==== debug ====
              std::cout << "    FieldNode " << i << ": " << child_schema->format << ' ' << child_schema->name << "\r\n";
              std::cout << "      node: length=" << node->length() << ", ";
              std::cout << "null_count=" << node->null_count() << "\r\n";
              // ==== debug ====

              auto child = out->children[i];
              child->length = node->length();
              child->null_count = node->null_count();
              
              child->n_buffers = 2;
              int format_len = strlen(child_schema->format);
              if ((format_len == 1 && (strncmp(child_schema->format, "u", 1) == 0 || strncmp(child_schema->format, "U", 1) == 0)) || (format_len == 2 && strncmp(child_schema->format, "vu", 2) == 0)) {
                  child->n_buffers = 3;
              }
              child->buffers = (const void **)malloc(sizeof(uint8_t *) * child->n_buffers);
              printf("      child_schema->format: %s\r\n", child_schema->format);
              printf("      child->n_buffers: %lld\r\n", child->n_buffers);

              printf("      buffer[%d]: ", buffer_index);
              auto buffer = buffers->Get(buffer_index);
              printf("offset=%lld, length=%lld\r\n", buffer->offset(), buffer->length());
              child->buffers[0] = (const uint8_t *)(((uint64_t)(uint64_t*)body_data) + buffer->offset());
              buffer_index++;

              printf("      buffer[%d]: ", buffer_index);
              buffer = buffers->Get(buffer_index);
              printf("offset=%lld, length=%lld\r\n", buffer->offset(), buffer->length());
              child->buffers[1] = (const uint8_t *)(((uint64_t)(uint64_t*)body_data) + buffer->offset());
              buffer_index++;

              if (child->n_buffers == 3) {
                  printf("      buffer[%d]: ", buffer_index);
                  buffer = buffers->Get(buffer_index);
                  printf("offset=%lld, length=%lld\r\n", buffer->offset(), buffer->length());
                  child->buffers[2] = (const uint8_t *)(((uint64_t)(uint64_t*)body_data) + buffer->offset());
                  buffer_index++;
              }
          }

          return ADBC_STATUS_OK;
      } else if (header_type == org::apache::arrow::flatbuf::MessageHeader::DictionaryBatch) {
          std::cout << "dictionary batch\r\n";
          // auto dictionary_batch = header->header_as_DictionaryBatch();
          // auto id = dictionary_batch->id();
          // auto data = dictionary_batch->data();
          return ADBC_STATUS_NOT_IMPLEMENTED;
      } else {
          // The columnar IPC protocol utilizes a one-way stream of binary messages of these types:
          //
          // - Schema
          // - RecordBatch
          // - DictionaryBatch
          std::cout << "unexpected format\r\n";
          return ADBC_STATUS_INTERNAL;
      }
  } else {
      // error?
      printf("unexpected header type\r\n");
      return ADBC_STATUS_INTERNAL;
  }
}

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
  session_ = std::make_shared<::google::cloud::bigquery::storage::v1::ReadSession>(*session);
  current_ = response_->begin();

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
  if (iterator->current_ == iterator->response_->end()) {
    return 0;
  }

  auto& row = *iterator->current_;
  if (!row.ok()) {
    return ADBC_STATUS_INTERNAL;
  }

  struct ArrowSchema * parsed_schema = nullptr;
  if (iterator->parsed_schema_ == nullptr) {
    parsed_schema = (struct ArrowSchema *)malloc(sizeof(struct ArrowSchema));
    memset(parsed_schema, 0, sizeof(struct ArrowSchema));
    
    int ret = iterator->get_schema(stream, parsed_schema);
    if (ret != ADBC_STATUS_OK) {
      return ret;
    }
  }

  auto& serialized_record_batch = row->arrow_record_batch().serialized_record_batch();
  printf("serialized_record_batch: length=%zu\r\n", serialized_record_batch.length());
  iterator->current_++;
  return parse_encapsulated_message(
    serialized_record_batch, 
    org::apache::arrow::flatbuf::MessageHeader::RecordBatch, 
    out, 
    parsed_schema);
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
  auto& serialized_schema = session->arrow_schema().serialized_schema();

  if (iterator->parsed_schema_) {
    memcpy(out, iterator->parsed_schema_, sizeof(struct ArrowSchema));
    return ADBC_STATUS_OK;
  }

  int ret = parse_encapsulated_message(
    serialized_schema,
    org::apache::arrow::flatbuf::MessageHeader::Schema, 
    out, 
    nullptr);
  if (ret != ADBC_STATUS_OK) {
    return ret;
  } else {
    iterator->parsed_schema_ = (struct ArrowSchema *)malloc(sizeof(struct ArrowSchema));
    memcpy(iterator->parsed_schema_, out, sizeof(struct ArrowSchema));
  }

  return ADBC_STATUS_OK;
}

void ReadRowsIterator::release(struct ArrowArrayStream* stream) {
  if (stream && stream->private_data) {
    auto* ptr = reinterpret_cast<std::shared_ptr<ReadRowsIterator>*>(stream->private_data);
    if (ptr) {
      if ((*ptr)->parsed_schema_) {
        free((*ptr)->parsed_schema_);
        (*ptr)->parsed_schema_ = nullptr;
      }
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

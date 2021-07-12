/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.templates;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.io.DynamicJdbcIO;
import com.google.cloud.teleport.templates.common.JdbcConverters;
import com.google.cloud.teleport.templates.common.BigQueryConverters.BigQueryReadOptions;
import com.google.cloud.teleport.templates.common.BigQueryConverters.BigQueryToEntity;
import com.google.cloud.teleport.templates.common.DatastoreConverters.DatastoreWriteOptions;
import com.google.cloud.teleport.templates.common.DatastoreConverters.WriteEntities;
import com.google.cloud.teleport.templates.common.ErrorConverters.ErrorWriteOptions;
import com.google.cloud.teleport.templates.common.ErrorConverters.LogErrors;
import com.google.datastore.v1.Entity;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Dataflow template which reads BigQuery data and writes it to Datastore. The source data can be
 * either a BigQuery table or a SQL query.
 */
public class BigQueryToJdbc {

  /**
   * Custom PipelineOptions.
   */
  public interface BigQueryToJdbcOptions
      extends BigQueryReadOptions, JdbcConverters.JdbcToBigQueryOptions, ErrorWriteOptions {}

/**
   * The {@link BigQueryToTFRecord#record2Example(SchemaAndRecord)} method uses takes in a
   * SchemaAndRecord Object returned from a BigQueryIO.read() step and builds a TensorFlow Example
   * from the record.
   */
  @VisibleForTesting
  protected static TableRow mapRecord(SchemaAndRecord schemaAndRecord) {
    Example.Builder example = Example.newBuilder();
    Features.Builder features = example.getFeaturesBuilder();
    GenericRecord record = schemaAndRecord.getRecord();
    for (TableFieldSchema field : schemaAndRecord.getTableSchema().getFields()) {
      Object fieldValue = record.get(field.getName());
      if (fieldValue != null) {
        Feature feature = buildFeature(fieldValue, field.getType());
        features.putFeature(field.getName(), feature);
      }
    }
    return example.build().toByteArray();
  }

  /**
   * Runs a pipeline which reads data from BigQuery and writes it to Datastore.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {

    BigQueryToDatastoreOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(BigQueryToDatastoreOptions.class);

    Pipeline pipeline = Pipeline.create(options);

    PCollection<TableRow> bigQueryToExamples =
        pipeline
            .apply(
                "RecordToExample",
                BigQueryIO.read(BigQueryToJdbc::mapRecord)
                    .fromQuery(options.getReadQuery())
                    .withCoder(ByteArrayCoder.of())
                    .withTemplateCompatibility()
                    .withoutValidation()
                    .usingStandardSql()
                    .withMethod(BigQueryIO.TypedRead.Method.DIRECT_READ)
                // Enable BigQuery Storage API
            )

            .apply(
                "Write from JdbcIO",
                DynamicJdbcIO.<TableRow>write()
                    .withDataSourceConfiguration(
                        DynamicJdbcIO.DynamicDataSourceConfiguration.create(
                                options.getDriverClassName(),
                                maybeDecrypt(options.getConnectionURL(), options.getKMSEncryptionKey()))
                            .withUsername(
                                maybeDecrypt(options.getUsername(), options.getKMSEncryptionKey()))
                            .withPassword(
                                maybeDecrypt(options.getPassword(), options.getKMSEncryptionKey()))
                            .withDriverJars(options.getDriverJars())
                            .withConnectionProperties(options.getConnectionProperties()))
                    .withQuery(vpQuery)
                    .withCoder(TableRowJsonCoder.of())
                    .withRowMapper(JdbcConverters.getResultSetToTableRow()))
    pipeline.run();
  }
}

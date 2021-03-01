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
import com.google.cloud.teleport.util.KMSEncryptedNestedValueProvider;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A template that copies data from a relational database using JDBC to an existing BigQuery table.
 */
public class JdbcToBigQueryLiveloPubSub {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcToBigQueryLiveloGCS.class);

  private static ValueProvider<String> maybeDecrypt(
      ValueProvider<String> unencryptedValue, ValueProvider<String> kmsKey) {
    return new KMSEncryptedNestedValueProvider(unencryptedValue, kmsKey);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   */
  public interface MyOptions extends PipelineOptions, JdbcConverters.JdbcToBigQueryOptions {
    @Description("Query that merge the data Id")
    ValueProvider<String> getQueryId();
    void setQueryId(ValueProvider<String> value);

    @Description("PubSub topic to send the payload")
    @Default.String("livelo-clm-merge")
    ValueProvider<String> getOutputTopic();
    void setOutputTopic(ValueProvider<String> value);
  }

  /**
   * Main entry point for executing the pipeline. This will run the pipeline asynchronously. If
   * blocking execution is required, use the {@link
   * JdbcToBigQuery#run(JdbcConverters.JdbcToBigQueryOptions)} method to start the pipeline and
   * invoke {@code result.waitUntilFinish()} on the {@link PipelineResult}
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {

    // Parse the user options passed from the command-line
    MyOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(MyOptions.class);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  private static PipelineResult run(MyOptions options) {
    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    LOG.info("Init Dataflow pipeline...");

    /*
     * Steps: 1) Read records via JDBC and convert to TableRow via RowMapper
     *        2) Append TableRow to BigQuery via BigQueryIO
     */
      /*
      * Step 1: Read records via JDBC and convert to TableRow
      *         via {@link org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper}
      */
    PCollection<TableRow> rows = pipeline
        .apply(
            "Read from JdbcIO",
            DynamicJdbcIO.<TableRow>read()
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
                .withQuery(options.getQuery())
                .withCoder(TableRowJsonCoder.of())
                .withRowMapper(JdbcConverters.getResultSetToTableRow()));

        /*
         * Step 2: Append TableRow to an existing BigQuery table
         */
        WriteResult writeResult = rows.apply(
            "Write to BigQuery",
            BigQueryIO.writeTableRows()
                .withoutValidation()
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                //.withJsonSchema(TableRowJsonCoder.encode(rows))
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withExtendedErrorInfo()
                .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
                .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory())
                .to(options.getOutputTable()));

       pipeline.apply("Create trigger file", Create.ofProvider(options.getQueryId(), StringUtf8Coder.of()))
       .apply("Format JSON message", MapElements.via(
          new SimpleFunction<String, String>() {
            @Override
            public String apply(String line) {
              return "{\"id\" : \"" + line + "\"}";
            }
          }))
       .apply("Wait write on BigQuery", Wait.on(writeResult.getFailedInserts()))
       .apply("Sending Pub/sub",PubsubIO.writeStrings().to(options.getOutputTopic()));
    // Execute the pipeline and return the result.
    PipelineResult res = pipeline.run();

    return res;
  }
}

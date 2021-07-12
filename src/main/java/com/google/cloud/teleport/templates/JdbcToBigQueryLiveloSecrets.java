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

import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;

import java.io.IOException;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.io.DynamicJdbcIO;
import com.google.cloud.teleport.templates.common.JdbcConverters;
import com.google.cloud.teleport.util.KMSEncryptedNestedValueProvider;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A template that copies data from a relational database using JDBC to an existing BigQuery table.
 */
public class JdbcToBigQueryLiveloSecrets {

  private static String secretTranslator(String secretName) {
    if (secretName.matches("projects/[0-9]+/secrets/.*")){
      try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
        AccessSecretVersionResponse response = client.accessSecretVersion(secretName);

        return response.getPayload().getData().toStringUtf8();
      } catch (IOException e) {
        throw new RuntimeException("Unable to read JDBC URL secret: " + secretName);
      }
    } else {
      return secretName;
    }
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
    JdbcConverters.JdbcToBigQueryOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(JdbcConverters.JdbcToBigQueryOptions.class);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  private static PipelineResult run(JdbcConverters.JdbcToBigQueryOptions options) {
    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    NestedValueProvider<String, String> jdbcUrlValueProvider =
        NestedValueProvider.of(options.getConnectionURL(), JdbcToBigQueryLiveloSecrets::secretTranslator);
    NestedValueProvider<String, String> jdbcUserValueProvider =
        NestedValueProvider.of(options.getUsername(), JdbcToBigQueryLiveloSecrets::secretTranslator);
    NestedValueProvider<String, String> jdbcPasswordValueProvider =
        NestedValueProvider.of(options.getPassword(), JdbcToBigQueryLiveloSecrets::secretTranslator);
    /*
     * Steps: 1) Read records via JDBC and convert to TableRow via RowMapper
     *        2) Append TableRow to BigQuery via BigQueryIO
     */
    pipeline
        /*
         * Step 1: Read records via JDBC and convert to TableRow
         *         via {@link org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper}
         */
        .apply(
            "Read from JdbcIO",
            DynamicJdbcIO.<TableRow>read()
                .withDataSourceConfiguration(
                    DynamicJdbcIO.DynamicDataSourceConfiguration.create(
                            options.getDriverClassName(), jdbcUrlValueProvider)
                        .withUsername(jdbcUserValueProvider)
                        .withPassword(jdbcPasswordValueProvider)
                        .withDriverJars(options.getDriverJars())
                        .withConnectionProperties(options.getConnectionProperties()))
                .withQuery(options.getQuery())
                .withCoder(TableRowJsonCoder.of())
                .withRowMapper(JdbcConverters.getResultSetToTableRow()))
        /*
         * Step 2: Append TableRow to an existing BigQuery table
         */
        .apply(
            "Write to BigQuery",
            BigQueryIO.writeTableRows()
                .withoutValidation()
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory())
                .to(options.getOutputTable()));

    // Execute the pipeline and return the result.
    return pipeline.run();
  }
}

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
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A template that copies data from a relational database using JDBC to an existing BigQuery table.
 */
public class TestSecrets {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcToBigQueryLiveloGCS.class);


  private static String secretTranslator(String secretName) {
    try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
      AccessSecretVersionResponse response = client.accessSecretVersion(secretName);

      return response.getPayload().getData().toStringUtf8();
    } catch (IOException e) {
      throw new RuntimeException("Unable to read JDBC URL secret: " + secretName);
    }
  }

  private static ValueProvider<String> maybeDecrypt(
      ValueProvider<String> unencryptedValue, ValueProvider<String> kmsKey) {
    return new KMSEncryptedNestedValueProvider(unencryptedValue, kmsKey);
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

  static class MyTestSecret extends DoFn<String, String> {
      ValueProvider<String> mySecretsUrl;
      ValueProvider<String> mySecretsUser;

      MyTestSecret(ValueProvider<String> secretsUrl, ValueProvider<String> secretsUser) {
          // Store the value provider
          this.mySecretsUrl = secretsUrl;
          this.mySecretsUser = secretsUser;
      }

      @ProcessElement
      public void processElement(ProcessContext c) {
         // Get the value of the value provider and add it to
         // the element's value.
         LOG.info("Url: " + mySecretsUrl.get());
         LOG.info("User: " + mySecretsUser.get());
         c.output(c.element());
      }
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
        NestedValueProvider.of(options.getConnectionURL(), TestSecrets::secretTranslator);
    NestedValueProvider<String, String> jdbcUserValueProvider =
        NestedValueProvider.of(options.getUsername(), TestSecrets::secretTranslator);
    NestedValueProvider<String, String> jdbcPasswordValueProvider =
        NestedValueProvider.of(options.getPassword(), TestSecrets::secretTranslator);
    /*
     * Steps: 1) Read records via JDBC and convert to TableRow via RowMapper
     *        2) Append TableRow to BigQuery via BigQueryIO
     */
     pipeline
      .apply("create test", Create.of("Hello World, Beam is fun"))
      .apply("log data", ParDo
          .of(new MyTestSecret(jdbcUrlValueProvider, jdbcUserValueProvider)));
   
    // Execute the pipeline and return the result.
    return pipeline.run();
  }
}

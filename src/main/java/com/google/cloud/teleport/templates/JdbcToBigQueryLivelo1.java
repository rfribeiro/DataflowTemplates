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
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.teleport.io.DynamicJdbcIO;
import com.google.cloud.teleport.templates.common.JdbcConverters;
import com.google.cloud.teleport.util.KMSEncryptedNestedValueProvider;
import java.util.UUID;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A template that copies data from a relational database using JDBC to an existing BigQuery table.
 */
public class JdbcToBigQueryLivelo1 {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcToBigQueryLivelo1.class);

  private static ValueProvider<String> maybeDecrypt(
      ValueProvider<String> unencryptedValue, ValueProvider<String> kmsKey) {
    return new KMSEncryptedNestedValueProvider(unencryptedValue, kmsKey);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   */
  public interface Options extends PipelineOptions, JdbcConverters.JdbcToBigQueryOptions {
    @Description("Query that merge the data")
    String getQueryMerge();

    void setQueryMerge(String value);
  }

  /**
   * Main entry point for executing the pipeline. This will run the pipeline asynchronously. If
   * blocking execution is required, use the {@link
   * JdbcToBigQuery#run(JdbcConverters.JdbcToBigQueryOptions)} method to start the pipeline and
   * invoke {@code result.waitUntilFinish()} on the {@link PipelineResult}
   *
   * @param args The command-line arguments to the pipeline.
   */
   @SuppressWarnings("serial")
  public static void main(String[] args) {

    // Parse the user options passed from the command-line
    Options options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(Options.class);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return None
   */
  private static void mergeResults(Options options) throws Exception {
    String sQueryMerge = ""
+"MERGE `rabbitmq_stream_data.cst_accounts` as a "
+"USING(select "
        +"acc_id,"
        +"acc_audit_cd as audit_date,"
        +"last_ingestion_datetime,"
        +"acc_audit_cd,"
        +"acc_audit_md,"
        +"acc_audit_mu,"
        +"acc_audit_cu,"
        +"acc_status,"
        +"acc_enrolment_date,"
        +"acc_enrolment_channel,"
        +"acc_points_balance,"
        +"acc_blocked_by,"
        +"acc_blocked_reason,"
        +"cast(acc_owner_cus_id as INT64) as acc_owner_cus_id,"
        +"cast(acc_tst_id as INT64) as acc_tst_id,"
        +"acc_delayed_points,"
        +"cast(acc_atp_id as INT64) as acc_atp_id,"
        +"dtype,"
        +"acc_blocked_date,"
        +"acc_points_exp_disabled,"
        +"acc_balance_type,"
        +"cast(acc_master_acc_id as INT64) as acc_master_acc_id,"
        +"cast(acc_prg_id as INT64) as acc_prg_id,"
        +"acc_loan_balance,"
        +"acc_merge_data,"
        +"cast(acc_promoter_cus_id as INT64) as acc_promoter_cus_id,"
        +"acc_corporate "
      +"from "
        +"`rabbitmq_stream_data.cst_accounts_stage`) as b "
   +"ON a.audit_date = b.audit_date "
  +"and a.acc_id = b.acc_id "
+"WHEN MATCHED THEN "
+"  UPDATE SET "
        +"a.last_ingestion_datetime = b.last_ingestion_datetime,"
        +"a.acc_audit_md = b.acc_audit_md,"
        +"a.acc_audit_mu = b.acc_audit_mu,"
        +"a.acc_audit_cu = b.acc_audit_cu,"
        +"a.acc_status = b.acc_status,"
        +"a.acc_enrolment_date = b.acc_enrolment_date,"
        +"a.acc_enrolment_channel = b.acc_enrolment_channel,"
        +"a.acc_points_balance = b.acc_points_balance,"
        +"a.acc_blocked_by = b.acc_blocked_by,"
        +"a.acc_blocked_reason = b.acc_blocked_reason,"
        +"a.acc_owner_cus_id = b.acc_owner_cus_id,"
        +"a.acc_tst_id = b.acc_tst_id,"
        +"a.acc_delayed_points = b.acc_delayed_points,"
        +"a.acc_atp_id = b.acc_atp_id,"
        +"a.dtype = b.dtype,"
        +"a.acc_blocked_date = b.acc_blocked_date,"
        +"a.acc_points_exp_disabled = b.acc_points_exp_disabled,"
        +"a.acc_balance_type = b.acc_balance_type,"
        +"a.acc_master_acc_id = b.acc_master_acc_id,"
        +"a.acc_prg_id = b.acc_prg_id,"
        +"a.acc_loan_balance = b.acc_loan_balance,"
        +"a.acc_merge_data = b.acc_merge_data,"
        +"a.acc_promoter_cus_id = b.acc_promoter_cus_id,"
        +"a.acc_corporate = b.acc_corporate "
+"WHEN NOT MATCHED BY TARGET THEN "
  +"INSERT (acc_id, "
          +"audit_date,"
          +"last_ingestion_datetime,"
          +"acc_audit_cd,"
          +"acc_audit_md,"
          +"acc_audit_mu,"
          +"acc_audit_cu,"
          +"acc_status,"
          +"acc_enrolment_date,"
          +"acc_enrolment_channel,"
          +"acc_points_balance,"
          +"acc_blocked_by,"
          +"acc_blocked_reason,"
          +"acc_owner_cus_id,"
          +"acc_tst_id,"
          +"acc_delayed_points,"
          +"acc_atp_id,"
          +"dtype,"
          +"acc_blocked_date,"
          +"acc_points_exp_disabled,"
          +"acc_balance_type,"
          +"acc_master_acc_id,"
          +"acc_prg_id,"
          +"acc_loan_balance,"
          +"acc_merge_data,"
          +"acc_promoter_cus_id,"
          +"acc_corporate) "
  +" VALUES (b.acc_id,"
          +"b.audit_date,"
          +"b.last_ingestion_datetime,"
          +"b.acc_audit_cd,"
          +"b.acc_audit_md,"
          +"b.acc_audit_mu,"
          +"b.acc_audit_cu,"
          +"b.acc_status,"
          +"b.acc_enrolment_date,"
          +"b.acc_enrolment_channel,"
          +"b.acc_points_balance,"
          +"b.acc_blocked_by,"
          +"b.acc_blocked_reason,"
          +"b.acc_owner_cus_id,"
          +"b.acc_tst_id,"
          +"b.acc_delayed_points,"
          +"b.acc_atp_id,"
          +"b.dtype,"
          +"b.acc_blocked_date,"
          +"b.acc_points_exp_disabled,"
          +"b.acc_balance_type,"
          +"b.acc_master_acc_id,"
          +"b.acc_prg_id,"
          +"b.acc_loan_balance,"
          +"b.acc_merge_data,"
          +"b.acc_promoter_cus_id,"
          +"b.acc_corporate); "
 +" drop table `rabbitmq_stream_data.cst_accounts_stage`;";
    options.setQueryMerge(sQueryMerge);

    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(
              options.getQueryMerge())
            .setUseLegacySql(false)
            .build();

    // Create a job ID so that we can safely retry.
    JobId jobId = JobId.of(UUID.randomUUID().toString());
    Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

    // Wait for the query to complete.
    queryJob = queryJob.waitFor();

    // Check for errors
    if (queryJob == null) {
      throw new RuntimeException("Job no longer exists");
    } else if (queryJob.getStatus().getError() != null) {
      throw new RuntimeException(queryJob.getStatus().getError().toString());
    }
  }
  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  private static PipelineResult run(Options options) {
    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    String sQuery = ""
+"SELECT"
+" ACC_ID"
+",SYSDATE AS last_ingestion_datetime"
+",ACC_AUDIT_CD"
+",ACC_AUDIT_MD"
+",ACC_AUDIT_MU"
+",ACC_AUDIT_CU"
+",ACC_STATUS"
+",ACC_ENROLMENT_DATE"
+",ACC_ENROLMENT_CHANNEL"
+",ACC_POINTS_BALANCE"
+",ACC_BLOCKED_BY"
+",ACC_BLOCKED_REASON"
+",ACC_OWNER_CUS_ID"
+",ACC_TST_ID"
+",ACC_DELAYED_POINTS"
+",ACC_ATP_ID"
+",DTYPE"
+",ACC_BLOCKED_DATE"
+",ACC_POINTS_EXP_DISABLED"
+",ACC_BALANCE_TYPE"
+",ACC_MASTER_ACC_ID"
+",ACC_PRG_ID"
+",ACC_LOAN_BALANCE"
+",ACC_MERGE_DATA"
+",ACC_PROMOTER_CUS_ID"
+",ACC_CORPORATE"
+" FROM COMARCH.CST_ACCOUNTS"
+" where (ACC_AUDIT_CD >= TO_DATE('17/02/2021','DD/MM/RRRR HH24:MI:SS') and "
+"       ACC_AUDIT_CD <  TO_DATE('18/02/2021','DD/MM/RRRR HH24:MI:SS')"
+"      ) or"
+"      (ACC_AUDIT_MD >= TO_DATE('17/02/2021','DD/MM/RRRR HH24:MI:SS') and "
+"       ACC_AUDIT_MD <  TO_DATE('18/02/2021','DD/MM/RRRR HH24:MI:SS')"
+"      )";

    org.apache.beam.sdk.options.ValueProvider<String> vpQuery = ValueProvider.StaticValueProvider.of(sQuery);

    LOG.info("Init Dataflow pipeline...");
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
    PipelineResult res = pipeline.run();
    
    try {
      LOG.info("wait Dataflow pipeline...");
      ((DataflowPipelineJob) res).waitUntilFinish();
      //String jobId = ((DataflowPipelineJob) res).getJobId();
      //DataflowClient client = DataflowClient.create(options);
      
      if (((DataflowPipelineJob) res).getState() == PipelineResult.State.DONE) {
          LOG.info("Dataflow job is finished. Merging results...");
          mergeResults(options);
          LOG.info("All done :)");
      }
    } catch (UnsupportedOperationException e) {
        LOG.error("UnsupportedOperationException happened");
        e.printStackTrace();
    } catch (Exception e){
        LOG.error("Exception happened: " + e.getMessage());
        e.printStackTrace();
    }
    return res;
  }
}

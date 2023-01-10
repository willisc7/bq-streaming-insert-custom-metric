## BigQuery Streaming Insert Buffer Custom Metric

### Steps
In this walkthrough we'll be using the following system table to build our custom metric: https://cloud.google.com/bigquery/docs/information-schema-tables

1. Navigate to the BigQuery console, create a dataset in BigQuery named `ds_streaming` and a table in that dataset named `tbl_users`
1. Make sure your user has Owner and BigQuery Editor roles on your project
    * **Note:** this is for demo purposes only. Using the Owner role is typically not a best practice.
1. Edit the `users_streaming_stub.py` and replace `PROJECT_ID` with the correct value
1. Start generating BQ insert activity
    ```
    while true; do python users_streaming_stub.py && sleep 5s; done
    ```
1. Open a new terminal, start populating the custom metric (replace `PROJECT_ID` with the correct value)
    ```
    while true; do python bq_custom_metric.py --project_id=PROJECT_ID && sleep 1m; done
    ```
1. Navigate to the Cloud Monitoring console, click on Metrics Explorer > Select a Metric > Global > Custom > BQ_Information_Schema_Metric
    * Set `Aggregator` to `sum`
    * Click **Save**
    * Select **New Dashboard**
    * Enter a name and click **Save**
1. Navigate to the Cloud Monitoring console, click on Alerting
    * Click on `EDIT NOTIFICATION CHANNELS`
    * Next to **Email** click **ADD NEW**
    * Enter Email address and name it `My Email`
    * Click **SAVE**
1. Navigate to the Cloud Monitoring console, click on Alerting
    * Click on **Create Alert Policy**
    * Click on `Select a metric > Global > Custom` and select `BQ_Information_Schema_Metric`
    * Set `Rolling window` to `1 min`
    * Click **Next**
    * In **Threshold value**, enter `50`
    * Click **Next**
    * In the **Notification channels** dropdown select `My Email`and click **OK**
    * In **Alert policy name**, enter **BQ Insert Buffer**
    * Click **CREATE POLICY**
1. As long as your script is still running you should receive an alert after the threshold passes 50 for 1 minute

### Contributors
* [Prasad Alle](https://github.com/prasadalle) for creating the scripts in this walkthrough

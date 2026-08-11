# The Architectural Paradigm of Spark Declarative Pipelines

### Purpose: 
Integrate Databricks Lakeflow Declarative Pipeline processing events stored in databricks with AWS CloudWatch for improved telemetry and alerting.

## Pull Architecture
Pulling with AWS Lambda by quering databricks sql warehouse event logs with status type 'ERROR' using event_log() sql function or from a quarantine table. 
AWS SNS will receive error events sent by Lambda and send email alerts to users.

Key Components:
1. Amazon EventBridge: Scheduled trigger (every X minutes).
2. AWS Lambda: The monitoring brain, assuming an IAM Role with SNS publish and CloudWatch logging permissions.
3. Databricks Serverless Compute: Executes a SQL query against the 'event_log()' or quarantine table of the 'cloudwatch_metrics_pull' pipeline.
4. Databricks Event Logs: Provides the 'linear story' of the pipeline's status within the UC catalog.
5. Amazon SNS: Sends real-time email alerts to some email address when errors are detected.
6. AWS CloudWatch Logs: Stores execution data for ad-hoc retrospective analysis.

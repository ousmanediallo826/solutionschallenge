import base64
import json
import os
import logging

# Import clients for services you want to control
from google.cloud import run_v2 # For Cloud Run services
from google.cloud import bigquery_datatransfer # For BigQuery Scheduled Queries (Data Transfer Service)
# from google.cloud import functions_v2 # If controlling other functions

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID") # This will be set by the CF environment

# Initialize clients globally if they will be used across invocations
try:
    run_client = run_v2.ServicesClient()
    bq_transfer_client = bigquery_datatransfer.DataTransferServiceClient()
    logger.info("GCP service clients initialized.")
except Exception as e:
    logger.error(f"Failed to initialize GCP service clients: {e}", exc_info=True)
    run_client = None
    bq_transfer_client = None


def handle_budget_alert(event, context):
    """
    Triggered by a message on the budget-alerts-topic Pub/Sub topic.
    """
    if not GCP_PROJECT_ID:
        logger.error("GCP_PROJECT_ID environment variable not set for the function.")
        return # Cannot proceed without project ID

    try:
        pubsub_message_data_str = base64.b64decode(event['data']).decode('utf-8')
        logger.info(f"Received budget alert message data: {pubsub_message_data_str}")
        
        budget_notification = json.loads(pubsub_message_data_str)

        budget_display_name = budget_notification.get("budgetDisplayName")
        cost_amount = budget_notification.get("costAmount")
        budget_amount = budget_notification.get("budgetAmount")
        currency_code = budget_notification.get("currencyCode")
        alert_threshold_exceeded = budget_notification.get("alertThresholdExceeded") # e.g., 0.5, 0.9, 1.0

        logger.info(
            f"Budget Alert: '{budget_display_name}' - "
            f"Cost: {cost_amount} {currency_code}, "
            f"Budget: {budget_amount} {currency_code}, "
            f"Threshold Exceeded: {alert_threshold_exceeded*100}%"
        )

        # --- Implement Your Cost Control Logic Here ---
        # Example: If 90% of budget is exceeded, scale down non-critical services

        if alert_threshold_exceeded >= 0.9: # 90% threshold
            logger.warning(f"Budget threshold >= 90% reached for '{budget_display_name}'. Taking action.")
            
            # Example 1: Scale down a specific Cloud Run service to 0 max instances (effectively stopping it)
            # You'd need to know the service name and region.
            # This is a simplified example; real updates are more complex with ServicesClient.
            service_to_scale_down = "crna-insights-api" # Example service name
            service_region = "us-central1" # Example region
            
            if run_client:
                try:
                    service_path = run_client.service_path(GCP_PROJECT_ID, service_region, service_to_scale_down)
                    service_config = run_client.get_service(name=service_path)
                    
                    # Modify the template to set max_instances to 0
                    # This is a bit simplified; you'd typically update the revision template
                    # and trigger a new deployment or update the service traffic splitting.
                    # For truly stopping, you might remove invoker permissions or delete.
                    # A simpler way to "stop" might be to update traffic to 0% for active revisions.
                    # For now, let's log that we would do something.
                    logger.info(f"ACTION (Simulated): Would attempt to scale down/stop Cloud Run service: {service_to_scale_down}")
                    # Actual implementation for setting max_instances to 0:
                    # service_config.template.scaling.max_instance_count = 0
                    # operation = run_client.update_service(service=service_config)
                    # logger.info(f"Update operation for {service_to_scale_down} started: {operation.operation.name}")

                except Exception as e_run:
                    logger.error(f"Failed to take action on Cloud Run service {service_to_scale_down}: {e_run}", exc_info=True)
            else:
                logger.error("Cloud Run client not available to take action.")


            # Example 2: Disable a BigQuery Scheduled Query
            # You need the Transfer Config ID (Scheduled Query ID)
            scheduled_query_to_disable = "projects/YOUR_PROJECT_ID/locations/YOUR_REGION/transferConfigs/YOUR_SCHEDULED_QUERY_ID" # Full path
            
            if bq_transfer_client:
                try:
                    transfer_config = bigquery_datatransfer.TransferConfig(
                        name=scheduled_query_to_disable.replace("YOUR_PROJECT_ID", GCP_PROJECT_ID).replace("YOUR_REGION", "us-central1"), # Adjust region
                        disabled=True
                    )
                    update_mask = {"paths": ["disabled"]}
                    bq_transfer_client.update_transfer_config(
                        transfer_config=transfer_config, update_mask=update_mask
                    )
                    logger.info(f"ACTION: Disabled BigQuery Scheduled Query: {scheduled_query_to_disable}")
                except Exception as e_bq_transfer:
                    logger.error(f"Failed to disable BigQuery Scheduled Query {scheduled_query_to_disable}: {e_bq_transfer}", exc_info=True)
            else:
                logger.error("BigQuery Transfer client not available to take action.")

            # Example 3: Send a critical Slack notification
            # (Would require 'requests' or 'slack_sdk' and Slack webhook URL in env vars)
            # send_slack_notification(f"CRITICAL: Budget alert for '{budget_display_name}'. Cost: {cost_amount}/{budget_amount} {currency_code}.")


        # Add more conditions for different thresholds (e.g., 100%)
        elif alert_threshold_exceeded >= 1.0: # 100% threshold
            logger.critical(f"CRITICAL: Budget threshold >= 100% reached for '{budget_display_name}'. Manual intervention likely required.")
            # send_slack_notification(f"EMERGENCY: Budget EXCEEDED for '{budget_display_name}'. Cost: {cost_amount}/{budget_amount} {currency_code}. Consider disabling billing.")
            # Disabling billing programmatically is possible but very risky.
            # from google.cloud import billing_v1
            # billing_client = billing_v1.CloudBillingClient()
            # project_billing_info = billing_client.get_project_billing_info(name=f"projects/{GCP_PROJECT_ID}")
            # if project_billing_info.billing_enabled:
            #     billing_client.update_project_billing_info(
            #         name=f"projects/{GCP_PROJECT_ID}",
            #         project_billing_info={"billing_account_name": ""} # Disables billing
            #     )
            #     logger.critical(f"ACTION: Billing programmatically disabled for project {GCP_PROJECT_ID}.")


    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from budget alert message: {e}. Raw data: {event.get('data')}")
    except Exception as e:
        logger.error(f"Unhandled error processing budget alert (event_id: {context.event_id}): {e}", exc_info=True)
        raise e # Re-raise to allow Pub/Sub to retry (though budget alerts usually don't need retry)

# Helper for Slack (example)
# def send_slack_notification(message):
#     slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL")
#     if slack_webhook_url:
#         payload = {"text": message}
#         try:
#             requests.post(slack_webhook_url, json=payload)
#             logger.info("Sent Slack notification.")
#         except Exception as e_slack:
#             logger.error(f"Failed to send Slack notification: {e_slack}")
#     else:
#         logger.warning("SLACK_WEBHOOK_URL not set. Cannot send Slack notification.")
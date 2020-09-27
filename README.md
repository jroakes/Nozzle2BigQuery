# Nozzle2BigQuery
Local or Google Cloud Function for pulling Nozzle ranking data into BigQuery


## Implementation:
1. Create a New Project in GCP. We used `nozzle-ranking-data`.
2. Activate BigQuery and Pubsub APIs.
3. Create Service Account with admin roles for Big Query. Make sure to send the service
    account email to Nozzle to add to your datasets.
4. Generate JSON key for service account.
5. Create Pubsub topic.  We used `nozzle-trigger-run`.
6. Create BigQuery dataset.  We used `client_ranking_data`.
7. Create table `config` in dataset and map to Google sheet with columns: client, nozzle_domain, and nozzle_dataset.
8. Create Python 3.7 Cloud Function. Set your Pubsub topic as the trigger. Timeout: 270s.
9. Add this file into main.py of the function.
10. A requirements below to requrements.txt.
11. Add your Service Account JSON as service_account.json.
12. Function structure: /main.py
                        /requirements.txt
                        /service-account.json
13. Save Cloud Function
14. Set a Cloud Schedule CRON tast k to send `{'job': 'run all'}` to your Pubsub topic on any interval. `0 1 * * *` is
    daily at 1am.
    
    
## requirements.txt:
    google-cloud-bigquery
    google-cloud-pubsub
    pandas
    tqdm

# 🛒 E-Commerce Real-Time Streaming Data Pipeline

A cloud-native **real-time e-commerce data engineering project** built using **Python, Google Pub/Sub, Apache Beam, Google Cloud Dataflow, BigQuery, Docker, Artifact Registry, Cloud Storage, and PowerShell**.

The project simulates an online retailer receiving customer orders continuously and demonstrates how streaming events can be ingested, processed, validated, stored, and prepared for analytics using a scalable cloud architecture.

---

## 📌 Table of Contents

1. #1-project-overview
2. #2-business-problem
3. #3-project-objectives
4. #4-architecture
5. #5-technology-stack
6. #6-google-cloud-project
7. #7-project-structure
8. #8-local-development-environment
9. #9-set-up-the-python-environment
10. #10-configure-google-cloud
11. #11-create-pubsub-topic
12. #12-create-pubsub-subscription
13. #13-build-the-python-producer
14. #14-test-pubsub
15. #15-create-bigquery-datasets
16. #16-bigquery-data-architecture
17. #17-build-the-apache-beam-pipeline
18. #18-raw-bigquery-schema
19. #19-run-apache-beam-locally
20. #20-run-the-producer
21. #21-validate-data-in-bigquery
22. #22-docker-and-dataflow-preparation
23. [Dockerfile](#23-dockerfileflow-flex-template-metadata
25. #25-artifact-registry
26. #26-cloud-storage
27. #27-iam-permissions
28. #28-troubleshooting
29. #29-planned-data-quality-layer
30. #30-planned-duplicate-detection
31. [Planned ed-curated-layer
32. [Planned Five-ve-minute-revenue-aggregation
33. [Planned Reporting Layer](#
34. [Planned Dashboard](#34-plannedarget-architecture
36. [Engineering Decisions](#36-engineering-decisionsated
38. [Reproducing the Project](t
39. [39-project-status
40. #40-learning-outcomes

---

# 1. Project Overview

This project implements a **real-time e-commerce streaming data platform on Google Cloud**.

The solution simulates an online retailer receiving customer orders continuously throughout the day.

Instead of waiting for a traditional batch process to run periodically, each order is published as an event to **Google Pub/Sub** and consumed by an **Apache Beam streaming pipeline**.

The pipeline processes the events and writes them into **BigQuery**, creating the foundation for near real-time operational analytics and reporting.

### Current Streaming Architecture

```text
Python Producer
      │
      ▼
Google Pub/Sub
      │
      ▼
Apache Beam
      │
      ▼
BigQuery RAW
```

The project is being developed incrementally toward a more complete production-style architecture containing:

- streaming ingestion
- data validation
- duplicate detection
- rejected-record handling
- curated analytical datasets
- five-minute revenue aggregations
- reporting tables
- dashboard visualisation
- containerised Dataflow deployment

---

# 2. Business Problem

An e-commerce business can receive thousands or millions of customer transactions throughout the day.

Traditional batch pipelines may only update analytical systems every few hours or overnight.

This creates a delay between:

```text
Customer places order
        ↓
Transaction is processed
        ↓
Data pipeline executes
        ↓
Reporting database updates
        ↓
Business sees the result
```

For operational teams, this delay can make it difficult to monitor:

- current revenue
- order volumes
- product performance
- customer purchasing behaviour
- unusual transaction patterns
- operational issues
- short-term sales trends

The objective of this project is therefore to design a streaming architecture capable of capturing and processing order events shortly after they are generated.

---

# 3. Project Objectives

The project aims to demonstrate an end-to-end cloud streaming data engineering workflow.

### Core Objectives

- Simulate continuous e-commerce transactions using Python
- Represent each transaction as a JSON event
- Publish events to Google Pub/Sub
- Consume events using Apache Beam
- Process the pipeline in streaming mode
- Write raw event data into BigQuery
- Maintain separate RAW, CURATED and MART data layers
- Introduce automated data-quality controls
- Detect duplicate transactions
- Generate five-minute business metrics
- Deploy the Beam pipeline to Google Cloud Dataflow
- Package the pipeline using Docker
- Store container images in Artifact Registry
- Build a reporting-ready data layer
- Visualise near real-time KPIs through a dashboard

---

# 4. Architecture

## Current Architecture

```text
┌───────────────────────┐
│    Python Producer    │
│  Simulated Orders     │
└───────────┬───────────┘
            │
            │ JSON events
            ▼
┌───────────────────────┐
│     Google Pub/Sub    │
│      Orders Topic     │
└───────────┬───────────┘
            │
            ▼
┌───────────────────────┐
│      Apache Beam      │
│  Streaming Pipeline   │
└───────────┬───────────┘
            │
            ▼
┌───────────────────────┐
│       BigQuery        │
│      RAW Orders       │
└───────────────────────┘
```

## Target Architecture

```text
                     E-COMMERCE EVENT SOURCE
                              │
                              ▼
                     ┌─────────────────┐
                     │ Python Producer │
                     └────────┬────────┘
                              │
                              ▼
                     ┌─────────────────┐
                     │ Google Pub/Sub  │
                     └────────┬────────┘
                              │
                              ▼
                  ┌────────────────────────┐
                  │ Apache Beam / Dataflow │
                  │ Streaming Processing   │
                  └────────────┬───────────┘
                               │
             ┌─────────────────┴─────────────────┐
             │                                   │
             ▼                                   ▼
     ┌───────────────┐                    ┌───────────────┐
     │ Valid Records │                    │ Invalid Data  │
     └───────┬───────┘                    │ / DLQ Layer   │
             │                            └───────────────┘
             ▼
     ┌─────────────────┐
     │ BigQuery RAW    │
     └────────┬────────┘
              │
              ▼
     ┌─────────────────┐
     │ Data Quality &  │
     │ Deduplication   │
     └────────┬────────┘
              │
              ▼
     ┌─────────────────┐
     │ BigQuery        │
     │ CURATED Layer   │
     └────────┬────────┘
              │
              ▼
     ┌─────────────────┐
     │ 5-Minute Sales  │
     │ Aggregations    │
     └────────┬────────┘
              │
              ▼
     ┌─────────────────┐
     │ BigQuery MART   │
     └─────────────────┘
```

---

# 5. Technology Stack

| Technology | Purpose |
|---|---|
| Python | Generates simulated e-commerce transactions |
| Google Pub/Sub | Real-time event ingestion and messaging |
| Apache Beam | Defines the streaming transformation pipeline |
| Google Cloud Dataflow | Managed execution environment for Apache Beam |
| BigQuery | Cloud analytical data warehouse |
| Docker | Packages the streaming application and dependencies |
| Artifact Registry | Stores container images |
| Cloud Storage | Stores Dataflow template specifications and supporting files |
| PowerShell | Local command-line automation and Google Cloud operations |
| Google Cloud CLI | Creates and manages GCP resources |
| JSON | Event exchange format |

---

# 6. Google Cloud Project

The solution requires a Google Cloud project with billing enabled.

Set the project used by the Google Cloud CLI:

```powershell
gcloud config set project YOUR_PROJECT_ID
```

Verify the active project:

```powershell
gcloud config get-value project
```

Enable the required APIs:

```powershell
gcloud services enable pubsub.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable dataflow.googleapis.com
gcloud services enable artifactregistry.googleapis.com
gcloud services enable storage.googleapis.com
```

> Replace project IDs, service-account names, regions and bucket names throughout this README with values appropriate for your own environment.

---

# 7. Project Structure

A recommended structure for the repository is:

```text
ecommerce-realtime-streaming/
│
├── producer/
│   └── producer.py
│
├── pipeline/
│   └── streaming_pipeline.py
│
├── sql/
│   ├── raw_validation.sql
│   ├── curated_orders.sql
│   └── revenue_5min.sql
│
├── dataflow/
│   └── metadata.json
│
├── Dockerfile
├── requirements.txt
├── .gitignore
└── README.md
```

As the project develops, additional folders can be introduced for:

```text
tests/
config/
docs/
dashboard/
```

---

# 8. Local Development Environment

The project can be developed locally before being deployed to Google Cloud.

### Prerequisites

Install:

- Python 3
- Google Cloud CLI
- Docker
- PowerShell
- Git
- a code editor such as Visual Studio Code

Verify the installations:

```powershell
python --version
gcloud --version
docker --version
git --version
```

---

# 9. Set Up the Python Environment

Create a virtual environment:

```powershell
python -m venv .venv
```

Activate it:

```powershell
.\.venv\Scripts\Activate.ps1
```

Upgrade pip:

```powershell
python -m pip install --upgrade pip
```

Install project dependencies:

```powershell
pip install apache-beam[gcp]
pip install google-cloud-pubsub
pip install google-cloud-bigquery
```

Alternatively:

```powershell
pip install -r requirements.txt
```

Example `requirements.txt`:

```text
apache-beam[gcp]
google-cloud-pubsub
google-cloud-bigquery
```

---

# 10. Configure Google Cloud

Authenticate the Google Cloud CLI:

```powershell
gcloud auth login
```

Configure Application Default Credentials for Python applications:

```powershell
gcloud auth application-default login
```

Set the project:

```powershell
gcloud config set project YOUR_PROJECT_ID
```

---

# 11. Create Pub/Sub Topic

Create a topic that will receive incoming e-commerce orders.

```powershell
gcloud pubsub topics create ecommerce-orders
```

The resulting event flow begins as:

```text
Python Producer
       │
       ▼
ecommerce-orders
```

---

# 12. Create Pub/Sub Subscription

Create a subscription:

```powershell
gcloud pubsub subscriptions create ecommerce-orders-sub `
    --topic=ecommerce-orders
```

The subscription can be used for testing and inspecting messages independently of the production streaming pipeline.

---

# 13. Build the Python Producer

The producer simulates orders being created continuously.

Example event structure:

```json
{
  "order_id": "ORD-100001",
  "customer_id": "CUST-2050",
  "product_id": "PROD-105",
  "quantity": 2,
  "unit_price": 49.99,
  "total_amount": 99.98,
  "order_status": "completed",
  "event_timestamp": "2026-10-08T09:30:00Z"
}
```

The producer is responsible for:

1. generating a transaction
2. converting the transaction to JSON
3. encoding the message
4. publishing the event to Pub/Sub
5. repeating the process to simulate continuous activity

Conceptually:

```text
Generate Order
      ↓
Convert to JSON
      ↓
Encode Message
      ↓
Publish to Pub/Sub
      ↓
Wait
      ↓
Generate Next Order
```

---

# 14. Test Pub/Sub

Run the producer:

```powershell
python producer/producer.py
```

Pull events from the test subscription:

```powershell
gcloud pubsub subscriptions pull ecommerce-orders-sub `
    --limit=5 `
    --auto-ack
```

A successful test confirms that:

```text
Python Producer
      │
      │ publish
      ▼
Pub/Sub Topic
      │
      ▼
Subscription
      │
      ▼
Message received
```

---

# 15. Create BigQuery Datasets

The target warehouse follows a layered approach.

Create the datasets:

```powershell
bq mk ecommerce_RAW
bq mk ecommerce_curated
bq mk ecommerce_mart
```
or you could also create them directly in the google cloud console.
These layers provide separation between ingestion, transformation and business reporting.

---

# 16. BigQuery Data Architecture

## RAW

Purpose:

> Store incoming source events with minimal transformation.

Example:

```text
ecommerce_RAW.orders
```

## CURATED

Purpose:

> Store cleaned, validated and deduplicated transactions.

Example:

```text
ecommerce_curated.orders
```

## MART

Purpose:

> Store aggregated business metrics optimised for reporting and dashboard consumption.

Examples:

```text
ecommerce_mart.revenue_5min
ecommerce_mart.product_performance
ecommerce_mart.order_metrics
```

The overall data flow becomes:

```text
Pub/Sub
   │
   ▼
RAW
   │
   ▼
CURATED
   │
   ▼
MART
   │
   ▼
BI / Dashboard
```

---

# 17. Build the Apache Beam Pipeline

Apache Beam provides the processing layer between Pub/Sub and BigQuery.

At a high level, the pipeline performs:

```text
Read Pub/Sub
      ↓
Decode Message
      ↓
Parse JSON
      ↓
Validate Record
      ↓
Transform Record
      ↓
Write to BigQuery
```

The pipeline is designed in Apache Beam but can be executed using different runners.

During development:

```text
Apache Beam
    │
    ▼
DirectRunner
```

For cloud deployment:

```text
Apache Beam
    │
    ▼
DataflowRunner
```

This separation allows pipeline logic to be tested locally before cloud deployment.

---

# 18. RAW BigQuery Schema

An example RAW schema is:

```text
order_id          STRING
customer_id       STRING
product_id        STRING
quantity          INTEGER
unit_price        FLOAT
total_amount      FLOAT
order_status      STRING
event_timestamp   TIMESTAMP
ingestion_time    TIMESTAMP
```

`event_timestamp` represents when the source event occurred.

`ingestion_time` represents when the event entered the analytical pipeline.

Keeping both timestamps makes it possible to analyse processing latency and event arrival patterns.

---

# 19. Run Apache Beam Locally

Start the streaming pipeline using the DirectRunner:

```powershell
python pipeline/streaming_pipeline.py `
    --runner DirectRunner `
    --project YOUR_PROJECT_ID `
    --input_subscription projects/YOUR_PROJECT_ID/subscriptions/ecommerce-orders-sub
```

The pipeline should remain running because this is a streaming workload.

---

# 20. Run the Producer

Open a second terminal and activate the virtual environment.

```powershell
.\.venv\Scripts\Activate.ps1
```

Run:

```powershell
python producer/producer.py
```

The flow should now be:

```text
Terminal 1
Apache Beam Streaming Pipeline
            ▲
            │
         Pub/Sub
            ▲
            │
Terminal 2
Python Producer
```

---

# 21. Validate Data in BigQuery

After the producer and pipeline are running, validate that records have arrived.

Example:

```sql
SELECT *
FROM `YOUR_PROJECT_ID.ecommerce_raw.orders`
ORDER BY event_timestamp DESC
LIMIT 100;
```

Validate the row count:

```sql
SELECT COUNT(*) AS total_orders
FROM `YOUR_PROJE*T_ID.ecommerce_raw.orders`;
```

C*eck revenue arriving through the s*ream:

```sql
SELECT
    SUM(total*amount) AS total_revenue
FROM `YOU*_PROJECT_ID.ecommerce_raw.orders`;*```

Check processing latency:

``*sql
SELECT
    order_id,
    event*timestamp,
    ingestion_time,
   *TIMESTAMP_DIFF(
        ingestion_*ime,
        event_timestamp,
    *   SECOND
    ) AS processing_late*cy_seconds
FROM `YOUR_PROJECT_ID.e*ommerce_raw.orders`
ORDER BY inges*ion_time DESC;
```

---

# 22. Doc*er and Dataflow Preparation

Local execution validates the pipeline logic, but the target architecture executes the streaming workload thro*gh Google Cloud Dataflow.

The pip*line is containerised so that:

- *Python runtime requirements are controlled
- dependencies are packaged*consistently
- deployments become *epeatable
- the Dataflow runtime i* isolated from the local developme*t environment

The deployment proc*ss becomes:

```text
Application C*de
       │
       ▼
Docker Image
*      │
       ▼
Artifact Registry*       │
       ▼
Dataflow Flex Te*plate
       │
       ▼
Dataflow Streaming Job
```

---

# 23. Dockerfile

Example repository structure:*
```text
ecommerce-realtime-stream*ng/
│
├── Dockerfile
├── requirements.txt
├── producer/
└── pipeline/*```

The Dockerfile packages:

- P*thon
- Apache Beam
- Google Cloud dependencies
- pipeline code
- requ*red configuration

Before pushing *he image, test the container local*y where possible.

---

# 24. Dataflow Flex Template Metadata

A Flex*Template allows the containerised *ipeline to be launched as a Datafl*w job using predefined parameters.*
Example parameters might include:*
```text
project
region
input_subscription
output_table
temp_location staging_location
```

The template*separates deployment configuration*from the underlying Python pipelin* implementation.

---

# 25. Artifact Registry

Create a Docker repos*tory:

```powershell
gcloud artifa*ts repositories create ecommerce-s*reaming `
    --repository-format=*ocker `
    --location=YOUR_REGION*```

Configure Docker authenticati*n:

```powershell
gcloud auth conf*gure-docker YOUR_REGION-docker.pkg*dev
```

Build the image:

```powe*shell
docker build -t ecommerce-st*eaming .
```

Tag the image:

```powershell
docker tag ecommerce-stre*ming `
    YOUR_REGION-docker.pkg.*ev/YOUR_PROJECT_ID/ecommerce-strea*ing/ecommerce-pipeline:latest
```
*Push the image:

```powershell
doc*er push `
    YOUR_REGION-docker.p*g.dev/YOUR_PROJECT_ID/ecommerce-st*eaming/ecommerce-pipeline:latest
`*`

---

# 26. Cloud Storage

A Clo*d Storage bucket can store Dataflo* template specifications and tempo*ary deployment resources.

Create * bucket:

```powershell
gcloud sto*age buckets create gs://YOUR_BUCKE*_NAME `
    --location=YOUR_REGION*```

Example structure:

```text
g*://YOUR_BUCKET_NAME/
│
├── templat*s/
├── staging/
└── temp/
```

---*
# 27. IAM Permissions

The Datafl*w worker identity requires permiss*on to access the resources used by*the pipeline.

Typical access requ*rements include:

```text
Dataflow*   │
   ├── Read Pub/Sub
   ├── Wr*te BigQuery
   ├── Read Artifact R*gistry
   └── Access Cloud Storage*```

A production environment shou*d follow the **principle of least ***vilege**, granting only the perm*ssions required by the workload.

*redentials and service-account key* should never be committed to GitH*b.

---

## 28. Troubleshooting**

Some common issues encountered when d*veloping streaming pipelines inclu*e:

## Authentication Errors

Check:

```powershell
gcloud auth list
*``

Check Application Default Cred*ntials:

```powershell
gcloud auth*application-default login
```

---*
## Incorrect Google Cloud Project*
Verify:

```powershell
gcloud con*ig get-value project
```

---

## Pub/Sub Messages Not Arriving

ChecK the topic:

```powershell
gcloud *ubsub topics list
```

Check subsc*iptions:

```powershell
gcloud pub*ub subscriptions list
```

---

##*BigQuery Records Not Appearing

Ch*ck:

- dataset name
- table name
-*project ID
- schema compatibility
* pipeline logs
- Dataflow logs
- s*rvice-account permissions

---

##*Docker Authentication Failure

Rec*nfigure Docker:

```powershell
gcl*ud auth configure-docker YOUR_REGI*N-docker.pkg.dev
```

---

## Sche*a Errors

Ensure that JSON fields *ap correctly to the destination Bi*Query schema.

For example:

```te*t
quantity       → INTEGER
unit_pr*ce     → FLOAT
total_amount   → FL*AT
event_timestamp → TIMESTAMP
```*
---

# 29. Planned Data Quality L*yer

The next stage introduces aut*mated validation before records ar* promoted to the curated dataset.
*Example quality checks include:

`*`text
order_id IS NOT NULL
custome*_id IS NOT NULL
product_id IS NOT *ULL
quantity > 0
unit_price >= 0
t*tal_amount >= 0
event_timestamp IS*NOT NULL
```

A useful additional *usiness rule is:

```text
total_am*unt ≈ quantity × unit_price
```

R*cords failing validation should be*separated from valid transactions *nstead of silently entering the re*orting layer.

Target flow:

```te*t
Incoming Event
      │
      ▼
V*lidation Rules
     / \
    /   \
*Valid   Invalid
   │        │
   ▼*       ▼
RAW /     Rejected
CURATE*    Records
```

---

# 30. Planne* Duplicate Detection

Streaming sy*tems may receive the same event mo*e than once.

`order_id` can there*ore be used as a business identifi*r for duplicate detection.

Exampl* detection query:

```sql
SELECT
 *  order_id,
    COUNT(*) AS occurrence_count
FROM `YOUR_PROJECT_ID.ec*mmerce_raw.orders`
GROUP BY order_*d
HAVING COUNT(*) > 1;
```

The cu*ated layer will be designed to retain the appropriate unique transaction according to the project's deduplication rules.

This prevents duplicated events from artificially increasing:

- revenue
- transaction counts
- product quantities
- customer metrics

---

# 31. Planned Curated Layer

The curated layer contains data that has passed defined quality controls.

```text
ecommerce_curated.orders
```

Expected characteristics include:

- required fields populated
- correct data types
- valid transaction values
- duplicate handling
- standardised timestamps
- consistent statuses
- reporting-ready structure

The architecture becomes:

```text
                       ┌──► Rejected Records
                       │
RAW Orders ─► Quality Checks
                       │
                       └──► CURATED Orders
```

---

# 32. Planned Five-Minute Revenue Aggregation

A key analytical feature will be revenue aggregation over five-minute windows.

Conceptually:

```text
10:00 → 10:05
10:05 → 10:10
10:10 → 10:15
10:15 → 10:20
```

Each window could calculate:

```text
total_revenue
total_orders
total_items
average_order_value
```

For example:

```text
Window              Orders    Revenue

10:00 - 10:05         85      £7,950
10:05 - 10:10         92      £8,530
10:10 - 10:15        103      £9,410
```

These metrics can form the basis of a near real-time operational dashboard.

---


# 33. Target Architecture

The completed solution is intended to follow this architecture:

```text
Python Order Generator
        │
        ▼
Google Pub/Sub
        │
        ▼
Apache Beam
        │
        ▼
Google Cloud Dataflow
        │
        ├──────────────► Invalid / Rejected Events
        │
        ▼
BigQuery RAW
        │
        ▼
Data Quality
        │
        ▼
Deduplication
        │
        ▼
BigQuery CURATED
        │
        ▼
5-Minute Aggregations
        │
        ▼
BigQuery MART
        │
        ▼
Dashboard / Analytics
```

Supporting deployment infrastructure:

```text
Docker
   │
   ▼
Artifact Registry
   │
   ▼
Dataflow Flex Template
   │
   ▼
Cloud Storage
   │
   ▼
Dataflow Deployment
```

---

# 34. Engineering Decisions

## Why Pub/Sub?

Pub/Sub separates event producers from downstream processing.

The producer only needs to publish the order event rather than directly managing analytical storage.

```text
Producer → Messaging Layer → Consumers
```

This reduces coupling between systems.

---

## Why Apache Beam?

Apache Beam allows streaming processing logic to be defined independently from the underlying execution environment.

The same pipeline logic can therefore be tested locally before being executed using Dataflow.

---

## Why BigQuery?

BigQuery provides the analytical storage layer for:

- raw events
- transformed transactions
- data-quality analysis
- aggregations
- reporting datasets

---

## Why Layer the Warehouse?

Separating data into:

```text
RAW → CURATED → MART
```

provides clear responsibilities for each layer.

### RAW
Preserves incoming events.

### CURATED
Provides cleaned and controlled data.

### MART
Provides business-focused analytical models.

---

## Why Docker?

Containerisation improves deployment consistency by packaging:

- application code
- dependencies
- runtime requirements

into a portable deployment unit.

---

## Why Separate Producer and Pipeline?

The producer represents the **source application**, while the Beam pipeline represents the **data engineering platform**.

Keeping them independent better reflects a real-world event-driven architecture.

---

# 37. Skills Demonstrated

This project demonstrates practical experience across several areas of modern data engineering.

### Data Engineering

- event-driven architecture
- streaming ingestion
- pipeline design
- data transformation
- data modelling
- data-quality validation
- deduplication


### Google Cloud

- Pub/Sub
- BigQuery
- Dataflow
- Cloud Storage
- Artifact Registry
- IAM
- Google Cloud CLI

### Programming

- Python
- JSON processing
- Apache Beam
- SQL
- PowerShell

### DevOps

- Docker
- container registries
- cloud deployment
- dependency management
- environment configuration

### Analytics Engineering

- RAW / CURATED / MART architecture
- business metric design
- windowed aggregations
- reporting-layer development
- dashboard-ready datasets

---



# 38. Learning Outcomes

This project demonstrates the transition from traditional analytics into **cloud data engineering and real-time data processing**.

Key learning outcomes include:

1. Understanding event-driven architectures
2. Designing streaming rather than batch-only pipelines
3. Publishing and consuming asynchronous events
4. Developing Apache Beam pipelines
5. Working with Google Cloud Pub/Sub
6. Building analytical storage models in BigQuery
7. Separating RAW, CURATED and MART data responsibilities
8. Implementing data-quality controls
9. Understanding duplicate-event challenges
10. Working with event time and ingestion time
11. Designing time-window aggregations
12. Containerising data pipelines
13. Deploying workloads onto managed cloud infrastructure
14. Managing cloud identities and permissions
15. Designing data specifically for downstream analytics and reporting

---

## 🔮 Future Improvements

Potential future extensions include:

- event schema versioning
- automated unit and integration tests
- CI/CD using GitHub Actions
- Terraform infrastructure provisioning
- dead-letter queue implementation
- enhanced pipeline monitoring
- Dataflow autoscaling optimisation
- BigQuery partitioning and clustering
- cost monitoring
- data lineage
- metadata management
- real-time anomaly detection
- customer segmentation
- machine-learning based order analytics

---

## 🔐 Security

Sensitive credentials are not stored in this repository.

The project should use:

- Google Cloud IAM
- Application Default Credentials
- service accounts where appropriate
- least-privilege access

Files containing secrets, credentials or local environment configuration should be excluded through `.gitignore`.

Example:

```gitignore
.venv/
__pycache__/
*.pyc
.env
*.json
.DS_Store
.vscode/
```

> Do not use a blanket `*.json` exclusion if JSON configuration or Dataflow metadata files need to be committed. In that situation, exclude credential files explicitly instead.

---

## 💡 Key Takeaway

This project demonstrates how a continuously generated business event can move through a modern cloud data platform:

```text
Business Event
      │
      ▼
Event Streaming
      │
      ▼
Cloud Processing
      │
      ▼
Data Warehouse
      │
      ▼
Data Quality
      │
      ▼
Analytical Model
      │
      ▼
Business Insight
```

The goal is not simply to move data from **Pub/Sub to BigQuery**, but to demonstrate how a streaming pipeline can be designed as part of a scalable, maintainable and analytics-ready data platform.

---

## 👤 Author

**Temitope Arigbede**

Performance & Reporting Analyst | Data Analytics | Data Engineering | Cloud & AI

---

⭐ If you found this project useful or interesting, feel free to star the repository.

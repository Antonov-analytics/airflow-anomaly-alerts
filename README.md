# Anomaly Detection & Alert System in Airflow

### 🧩 Overview
This project implements an **automated anomaly detection and alert system** built with **Apache Airflow**, **ClickHouse**, and **Telegram Bot API**.

The system continuously monitors key application metrics, including views, likes, CTR, and active users — across both the **news feed** and **messenger**.  
Using a rolling **interquartile range (IQR)** method, the DAG detects abnormal deviations and automatically sends **alerts to Telegram** with visualizations and dashboard links built in Apache Superset.

All steps (extract → detect → visualize → alert) are fully automated and orchestrated by a single Airflow DAG running every 15 minutes.

---

### DAG Description
The DAG `dag_aleksandr_antonov_hnm5755_bot_alert` executes every **15 minutes** and performs two main tasks:

1. **Extract_metrics** - queries ClickHouse for aggregated metrics over 15-minute intervals, such as:  
   - Views;  
   - Likes;  
   - CTR (likes / views);  
   - Unique users in feed;  
   - Sent messages;  
   - Unique users in messenger.

2. **Check_and_alert** - checks each metric for anomalies using a rolling IQR-based algorithm.  
   If a value goes beyond the confidence bounds, a **Telegram alert** is triggered with:  
   - Current value and deviation from the previous point;  
   - Time-series chart with thresholds;  
   - Direct link to the Superset dashboard.

---

### Schedule & Configuration

| Parameter | Value |
|------------|--------|
| **Owner** | `aleksandr_antonov_hnm5755` |
| **Start Date** | `2025-10-07` |
| **Schedule Interval** | `*/15 * * * *` |
| **Retries** | 1 |
| **Retry Delay** | 5 minutes |
| **Database** | ClickHouse (simulator_20250820) |

---

### DAG Visualization

Below is an example of the DAG in **Airflow**, showing its automatic execution and monitoring:

![Airflow DAG](./Bot%20alert%20airflow.png)

---

### Example Alerts

#### Telegram Alert Example  
The Telegram bot sends detailed anomaly alerts with metric values and visual plots:

![Telegram Alert](./Telegram%20report.png)

---

#### Example of Anomaly Detection  
Visualization of detected anomalies for the metric *likes*:

![Anomaly Metric](./The%20anomaly%20-%20metric%20likes.jpg)

---

#### Key Application Metrics  
Metrics monitored by the DAG include activity in both the **feed** and **messenger**:

![Key Metrics](./Key%20app%20metrics.jpg)

---

### ⚙️ Tools & Technologies
![Python](https://img.shields.io/badge/Python-20A5A8?style=flat&logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Airflow-20A5A8?style=flat&logo=apacheairflow&logoColor=white)
![ClickHouse](https://img.shields.io/badge/ClickHouse-20A5A8?style=flat&logo=clickhouse&logoColor=white)
![Apache Superset](https://img.shields.io/badge/Apache%20Superset-20A5A8?style=flat&logo=apache-superset&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-20A5A8?style=flat&logo=pandas&logoColor=white)
![NumPy](https://img.shields.io/badge/NumPy-20A5A8?style=flat&logo=numpy&logoColor=white)
![Matplotlib](https://img.shields.io/badge/Matplotlib-20A5A8?style=flat&logo=plotly&logoColor=white)
![Seaborn](https://img.shields.io/badge/Seaborn-20A5A8?style=flat&logo=python&logoColor=white)
![Telegram API](https://img.shields.io/badge/Telegram%20API-20A5A8?style=flat&logo=telegram&logoColor=white)
![Jupyter](https://img.shields.io/badge/Jupyter-20A5A8?style=flat&logo=jupyter&logoColor=white)

---

### Usage
All materials are provided for reference and demonstration purposes.  
Database connections and Airflow runtime are **not required** to explore the project.

To review:
1. Open **`dag_aleksandr_antonov_hnm5755_bot_alert.py`** — full DAG implementation.  
2. View **`Bot alert airflow.png`** — visualization of DAG structure.  
3. Explore `.jpg` and `.png` files to see example outputs and alerts.  
4. The project illustrates the logic and structure of a real-time alerting system.

---

### 📂 Repository Structure

- `dag_aleksandr_antonov_hnm5755_bot_alert.py`   # main Airflow DAG.  
- `Bot alert airflow.png`                        # Airflow DAG execution graph.  
- `Key app metrics.jpg`                          # monitored application metrics visualization.  
- `The anomaly - metric likes.jpg`               # example of anomaly detection.  
- `Telegram report.png`                          # sample Telegram alert message.  
- `README.md`                                    # project documentation.

---

### Author
**Aleksandr Antonov**  
📊 Product & Data Analyst  
🎓 [Karpov.Courses — Data Analyst Simulator](https://karpov.courses)

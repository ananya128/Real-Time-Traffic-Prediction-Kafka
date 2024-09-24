# Real-Time-Traffic-Prediction-Kafka

This repository contains the implementation for real-time traffic prediction using Apache Kafka for data streaming and a predictive model for forecasting traffic flow.

## Project Overview

The goal of this project is to build a real-time traffic flow prediction system using streaming data from multiple sensor locations. The system ingests the data through Apache Kafka, processes the data, and trains a predictive model to forecast traffic volume at sensor locations.

The project is divided into three phases:

- **Phase 1: Kafka Setup**
- **Phase 2: Exploratory Data Analysis (EDA)**
- **Phase 3: Traffic Flow Prediction**

## Repository Structure

The repository is organized as follows:

- `phase_1_kafka_setup/`: Contains all Kafka producer and consumer scripts, along with the necessary code to stream traffic data using Apache Kafka.
- `phase_2_eda/`: Contains the Jupyter notebooks used for performing Exploratory Data Analysis (EDA) on the traffic data and the EDA report.
- `phase_3_model_prediction/`: Contains the scripts for model implementation and prediction using the processed features.
- `final_report/`: Includes the final report summarizing the project’s findings and performance.


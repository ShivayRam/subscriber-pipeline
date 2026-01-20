# 📬 Subscriber Pipeline

This repository contains a data engineering pipeline that cleans and standardizes subscriber data from a raw SQLite database and prepares cleaned outputs for analysis and reporting. It also includes an exploratory Jupyter notebook to inspect and validate both raw and processed data.

---

## 🚀 Project Overview

**Subscriber Pipeline** is designed to:

- Load **raw subscriber data** from an SQLite database (`cademycode.db`)
- Clean, transform, and enrich the data
- Write cleansed output to a target directory (`prod/`)
- Track changes across runs with a changelog
- Provide an interactive notebook for exploration and validation

This project demonstrates essential data engineering skills including ETL design, database interaction, data validation, and reproducible pipelines.

---

## 📁 Repository Structure

subscriber-pipeline/
├── data/
│ └── raw/
│ └── cademycode.db # Raw input database
├── notebooks/
│ └── explore_cademycode_db.ipynb # Data exploration notebook
├── prod/
│ ├── cademycode_cleansed.db # Cleansed database output
│ └── cademycode_cleansed.csv # Cleansed CSV output
├── src/
│ └── pipeline.py # Main pipeline script
├── tests/ # (Optional) Tests directory
├── .gitignore # Files to ignore in Git
├── changelog.md # Versioned pipeline changelog
├── README.md # This file
└── script.sh # Bash script to run & promote pipeline

---

## 🧠 Key Components

### 🛠 `src/pipeline.py`

This is the main Python pipeline that:

- Connects to the raw SQLite database
- Cleans and standardizes student, career, and job data
- Writes cleansed data to:
  - `prod/cademycode_cleansed.db`
  - `prod/cademycode_cleansed.csv`
- Updates the project `changelog.md` with version and counts

Run this pipeline to build your cleansed outputs.

---

### 📊 `notebooks/explore_cademycode_db.ipynb`

A Jupyter notebook that:

- Connects to the raw and cleansed SQLite databases
- Performs exploratory data analysis
- Demonstrates cleaning steps interactively
- Generates visualizations and insights

Paths inside the notebook are **project-relative**, so it works regardless of where Jupyter is started.

---

### 📝 `changelog.md`

Stores versioned changelogs for each pipeline run. Each version block includes:

- Version number (e.g., `0.0.1`)
- Description of changes (e.g., rows added, missing records)

This helps track how the data evolves over subsequent runs.

---

### 🐚 `script.sh`

A Bash wrapper that:

- Runs the Python pipeline
- Compares the current `changelog.md` version against the `prod/changelog.md`
- Promotes cleansed output files (`.db`, `.csv`, `changelog`) to `prod/` if updated

Use this script to manage your pipeline runs and promotion workflow.

---

## 🤖 Setup & Execution

### 📌 Clone the Repository

```bash
git clone https://github.com/<your-username>/subscriber-pipeline.git
cd subscriber-pipeline
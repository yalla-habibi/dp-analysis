# Disinfo Lab

Disinfo Lab is a small research pipeline for collecting articles from a WordPress-based source, extracting readable text, labeling the content with a local LLM, and reviewing the results in a Streamlit dashboard.

The project is designed for narrative and stance analysis. It focuses on questions like:

- which actors are described positively or negatively
- what persuasion or propaganda techniques appear in the text
- what short evidence snippets support the label
- how those patterns change across articles over time

The current workflow is:

1. ingest articles from a WordPress site
2. store article metadata and cleaned text in SQLite
3. send selected article text to Ollama for structured labeling
4. inspect the output in a dashboard


## What The Project Contains

- [scripts/ingest.py](/Users/daniel/Documents/Code/DP-analysis/scripts/ingest.py): CLI script for downloading recent articles
- [scripts/label.py](/Users/daniel/Documents/Code/DP-analysis/scripts/label.py): CLI script for generating LLM labels
- [dashboard/app.py](/Users/daniel/Documents/Code/DP-analysis/dashboard/app.py): Streamlit analytics dashboard
- [disinfo_lab/pipeline.py](/Users/daniel/Documents/Code/DP-analysis/disinfo_lab/pipeline.py): core ingest and labeling flow
- [disinfo_lab/llm_label.py](/Users/daniel/Documents/Code/DP-analysis/disinfo_lab/llm_label.py): prompt, Ollama request logic, and label normalization
- [disinfo_lab/parse.py](/Users/daniel/Documents/Code/DP-analysis/disinfo_lab/parse.py): HTML fetch and text extraction
- [disinfo_lab/storage.py](/Users/daniel/Documents/Code/DP-analysis/disinfo_lab/storage.py): SQLite and CSV storage helpers


## Requirements

- Python 3.10+
- Ollama running locally or on a reachable host
- a model available in Ollama, for example `llama3.1:latest`


## Installation

Create a virtual environment and install the project:

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -U pip
python3 -m pip install -e .
```


## Configuration

The project uses environment variables for configuration.

Important variables:

```bash
export DISINFO_BASE_URL="https://d1kgvz5kzmht23.cloudfront.net/"
export DISINFO_DB_URL="sqlite:///data/disinfo_lab.sqlite3"
export OLLAMA_BASE_URL="http://localhost:11434"
export OLLAMA_MODEL="llama3.1:latest"
```

Optional variables:

```bash
export DISINFO_DATA_DIR="data"
export DISINFO_WP_API_BASE=""
export DISINFO_TIMEOUT_S="25"
export OLLAMA_TIMEOUT_S="120"
```

Notes:

- if `DISINFO_WP_API_BASE` is empty, the code will use `DISINFO_BASE_URL + wp-json/wp/v2/`
- the default database is stored in `data/disinfo_lab.sqlite3`
- CSV mirrors are stored in `data/articles.csv` and `data/llm_labels.csv`


## How To Replicate The Pipeline

### 1. Start Ollama

Make sure Ollama is running and the selected model is available:

```bash
ollama serve
ollama list
```

If needed, pull the model:

```bash
ollama pull llama3.1:latest
```


### 2. Ingest Articles

Fetch the latest WordPress posts:

```bash
python3 scripts/ingest.py --category 9 --limit 50
```

Arguments:

- `--category`: WordPress category ID
- `--limit`: number of posts to fetch

The ingest step:

- collects post URLs from the WordPress API
- downloads article HTML
- extracts metadata and clean text
- saves results to SQLite
- mirrors the SQLite tables to CSV


### 3. Label Articles

Run the LLM labeling step:

```bash
python3 scripts/label.py --task stance_influence_v1 --batch 200
```

Arguments:

- `--task`: label set name stored in the database
- `--batch`: number of recent articles to consider
- `--category-filter`: optional article category filter

The labeler:

- prepares a focused excerpt from article text
- detects likely stance axes
- sends the prompt to Ollama
- expects structured JSON back
- stores the label in `llm_labels`

Using a new task name is recommended when you change the prompt or labeling logic, for example:

```bash
python3 scripts/label.py --task stance_influence_v2 --batch 200
```


### 4. Open The Dashboard

Start the Streamlit app:

```bash
streamlit run dashboard/app.py
```

The dashboard lets you:

- filter by category, confidence, text query, and techniques
- inspect stance distributions across entities
- view stance trends over time
- inspect the raw JSON label for a single article
- export filtered records as CSV


## Running A Fresh End-To-End Rebuild

If you want to restart from empty local storage:

```bash
rm data/disinfo_lab.sqlite3 data/articles.csv data/llm_labels.csv
python3 scripts/ingest.py --category 9 --limit 50
python3 scripts/label.py --task fresh_run_v1 --batch 200
streamlit run dashboard/app.py
```


## Label Output Structure

The current labeler aims to return JSON with fields like:

- `language`
- `target_entities`
- `target_audience_guess`
- `stance`
- `techniques`
- `rationale`
- `evidence`
- `confidence`
- `notes`

The `stance` object contains scores from `-2` to `2` for entities such as:

- `UE`
- `NATO`
- `USA`
- `Ukraina`
- `Rosja`
- `Niemcy`
- `Litwa`
- `Bialorus`
- `Rzad_Polski`


## Storage

The project uses SQLite as the working database and CSV files as a mirror/export format.

Main files:

- `data/disinfo_lab.sqlite3`
- `data/articles.csv`
- `data/llm_labels.csv`


## Troubleshooting

### `Ollama is not reachable`

Check that Ollama is running and that `OLLAMA_BASE_URL` is correct:

```bash
ollama serve
curl http://localhost:11434/api/tags
```


### `NOT NULL constraint failed: llm_labels.created_at`

This can happen if the database schema comes from an older version of the project and inserts do not provide timestamps. The current code now writes timestamps explicitly, so rerunning with the latest code should fix it.


### Dashboard starts but shows no labels

Make sure you ran the labeling step and that the selected `task` and `model` in the sidebar match existing records.


## Project Goal

The goal of this repository is not to build a production-grade disinformation detector. It is a lightweight analysis environment for experimenting with:

- article collection
- text cleaning
- LLM-based annotation
- stance analysis
- dashboard-driven review

It is best understood as a reproducible research and exploration tool.

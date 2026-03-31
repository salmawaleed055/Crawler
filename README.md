# Data.gov Dashboard
Dashboard link: https://crawler-4zztb6gnqzgckaslxwqhb2.streamlit.app/
<img width="946" height="432" alt="image" src="https://github.com/user-attachments/assets/1eb1c365-167c-42e8-a956-9b45dc7b0f23" />


An interactive Streamlit dashboard for analyzing 2,000 datasets from the data.gov API.

**Author:** Salma Elmarakby  
**ID:** 900232658  
**Project:** Milestone 2

## Overview

This project provides a comprehensive dashboard for exploring and analyzing datasets from data.gov. It includes data collection via API, database population, and interactive visualizations powered by Plotly and Streamlit.

## Project Structure

```
├── dashboard.py                          # Main Streamlit dashboard application
├── Crawler.py                  # Complete data collection and processing pipeline
├── populate_database_redesigned.py       # Database population script
├── run_pipeline.py                       # Pipeline runner
├── requirements.txt                      # Python dependencies
├── Milestone 2 - DB populated (1).sql    # Database schema and initial data
├── users (1).csv                         # User data file
└── output/                               # Output directory
    ├── datasets/                         # Processed dataset files
    │   ├── dataset_tags.csv
    │   └── datasets.csv
    ├── raw_data/                         # Raw API responses
    │   └── crawled_datasets_api.csv
    ├── reference/                        # Reference data
    │   ├── organizations.csv
    │   └── tags.csv
    └── users/                            # User-related data
        ├── usage.csv
        └── users_final.csv
```

## Installation

### Prerequisites
- Python 3.13+
- Virtual environment (recommended)

### Setup

1. Clone the repository
2. Create and activate a virtual environment:
   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Run the Dashboard

```bash
streamlit run dashboard.py
```

The app will be available at `http://localhost:8501`

### Run the Data Pipeline

To collect and process data:

```bash
python complete_pipeline.py
```

To populate the database:

```bash
python populate_database_redesigned.py
```

## Dependencies

- **streamlit** - Interactive web app framework
- **pandas** - Data manipulation and analysis
- **plotly** - Interactive visualizations
- **requests** - HTTP library for API calls
- **numpy** - Numerical computing
- **mysql-connector-python** - MySQL database connectivity

## Notes

- Ensure database credentials are properly configured for data population scripts
- API responses are cached in `output/raw_data/`
- Processed datasets are stored in `output/datasets/`

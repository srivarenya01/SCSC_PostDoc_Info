# AgriLife Postdoc Scraper

An automated tool to scrape the AgriLife People directory for postdoc contact details, identify those supervised by SCSC faculty, and track new additions over time.

## Features

- **Recursive Scraping**: Navigates through AgriLife units and subunits to find employees with postdoc-related titles.
- **Supervisor Extraction**: Fetches supervisor names and contact details for each postdoc.
- **SCSC Faculty Filtering**: Cross-references supervisors with the SCSC faculty list (fetched from Google Sheets or a local file).
- **Change Tracking**: Compares the current run with previous results in the `PastResults/` directory to tag postdocs as `NEW` or `OLD`.
- **Automated Scheduling**: Includes a GitHub Actions workflow to run the scraper **daily** and commit results back to the repository.

## Project Structure

- `agrilife_postdoc_scraper.py`: The main script that performs both scraping and post-processing.
- `TAMU_SCSC_Faculty.xlsx`: Local copy of the SCSC faculty list (downloaded from Google Sheets).
- `agrilife_postdocs.csv`: Full output containing all discovered postdocs across AgriLife.
- `scsc_postdocs.csv`: Filtered CSV output for SCSC-supervised postdocs, including `first_seen_date`.
- `PastResults/`: Archive folder where full CSV results are saved with a datestamp for historical comparison.
- `.github/workflows/scraper.yml`: GitHub Actions configuration for automated biweekly runs.

## Local Setup

### Prerequisites

- Python 3.8+
- Recommended: A virtual environment

```bash
# Create and activate a virtual environment
python -m venv venv
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate
```

### Installation

Install the required dependencies:

```bash
pip install requests beautifulsoup4 pandas openpyxl
```

## Usage

Run the scraper with default settings:

```bash
python agrilife_postdoc_scraper.py
```

### Optional Arguments

- `--output`: Path for the main CSV output (default: `agrilife_postdocs.csv`).
- `--faculty-xlsx`: Google Sheets URL or local path to the faculty Excel file (default: Google Sheets URL).
- `--scsc-output`: Path for the filtered Excel output (default: `scsc_postdocs.xlsx`).
- `--past-dir`: Folder for archived results (default: `PastResults`).
- `--max-workers`: Number of concurrent threads for fetching profile pages (default: `12`).

## Automation (GitHub Actions)

The included GitHub Action is configured to:
1. Run on the **1st and 15th** of every month.
2. Install dependencies and execute the scraper.
3. Automatically commit and push updated CSVs, Excel files, and archives back to the repository.

You can also trigger a run manually via the **Actions** tab in your GitHub repository by selecting the "SCSC Postdoc Scraper" workflow and clicking "Run workflow".

## Output Columns (CSV)

The `scsc_postdocs.csv` file includes the following columns:
- `first_name`: Postdoc first name (all name words except the last)
- `last_name`: Postdoc last name (last word of the cleaned name)
- `email`: Postdoc email
- `phone`: Postdoc phone number
- `title`: Postdoc job title
- `unit_name`: Department/Unit name
- `person_url`: Link to postdoc profile
- `supervisor_name`: Name of the supervisor
- `supervisor_url`: Link to supervisor profile
- `supervisor_phone`: Supervisor contact phone
- `first_seen_date`: The date (`YYYY-MM-DD`) when this postdoc was first observed by the scraper.

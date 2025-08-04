# 🍻 Belgian Beers Pipeline & GTM Simulation

**Simulating a go‑to‑market play using scraped + structured beer industry data.  
Built for showcasing data analysis skills in Python → BigQuery → dbt → Hex → Actionable Insights.**

---

## 🎯 Project Overview

Explain the problem: sourcing Belgian beer data to identify potential partnership regions, high-quality breweries, and strategic recommendations—just like Glide would segment users or target expansion regions.

---

## 📁 Architecture Diagram

![Architecture Diagram](./architecture.drawio.png)

---

## 🚦 Data Sources

- **BeerAdvocate**: ratings, styles, brewery names, ABV
- **Kaggle – Beers & Reviews**: clean dataset of beer reviews
- **Belgenbier / Wikipedia**: list of 500+ Belgian breweries with province and 2000+ beers
- **Data.gov.be CKAN API**: optional enrichment using tourism or waste data

---

## 🧪 Project Structure

```bash
belgian-brewery/
├── README.md
├── architecture_diagram.png
├── data/
│   ├── kaggle_beer_reviews.csv
│   ├── belgenbier.csv
│   ├── wikipedia_breweries.csv
│   └── beeradvocate_ratings.csv
├── src/
│   ├── __init__.py
│   ├── ingest/
│   │   ├── __init__.py
│   │   ├── beeradvocatescraper.py
│   │   ├── kaggleapi.py
│   │   └── belgenbierscraper.py
│   ├── transform/
│   │   ├── __init__.py
│   │   ├── bigquery_loader.py
│   │   ├── geolocator.py
│   │   └── scoring.py
│   └── util/
│       ├── __init__.py
│       ├── logger.py
│       └── errors.py
├── notebook/
├── dashboard/
├── requirements.txt
├── .gitignore
├── .env
└── LICENSE
```

---

## ⚙️ Run the Pipeline

Quick start:

```bash
git clone https://github.com/sam0per/belgian-brewery.git
cd belgian-brewery
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Start data ingestion:
python -m src.ingest.kagaload
python -m src.ingest.beerscraper --limit 100
python -m src.ingest.govapi

# Run transformations:
cd analytics
dbt seed  # if needed
dbt run
dbt test

# Launch dashboard:
cd dashboard
streamlit run dashboard.py
# or open Hex URL in browser

# View and update README:
nvim README.md  # or your preferred editor
```
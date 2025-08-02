# 🍻 Belgian Beers Pipeline & GTM Simulation

**Simulating a go‑to‑market play using scraped + structured beer industry data.  
Built™ for Glide’s Data Analyst role: Python → BigQuery → dbt → Hex → Narrative.**

---

## 🎯 Project Overview

Explain the problem: sourcing Belgian beer data to identify potential partnership regions, high-quality breweries, and strategic recommendations—just like Glide would segment users or target expansion regions.

---

## 📁 Architecture Diagram

![Architecture Diagram](./img/architecture.drawio.png) 
Highlights: Optional LLM via Ollama for name-cleaning, Python scraper layer → Raw tables in BigQuery → dbt transformations → Marketing‑style dashboard in Hex → Video walkthrough._

---

## 🚦 Data Sources

- **BeerAdvocate**: ratings, styles, brewery names, ABV (scraped reliably)
- **Kaggle – Beers & Reviews**: clean dataset of beer reviews (~18k distinct beers) :contentReference[oaicite:23]{index=23}
- **Belgenbier / Wikipedia**: list of ~1,200 Belgian breweries with province
- **Data.gov.be CKAN API**: optional enrichment using tourism or waste data

---

## 🧪 Project Structure

```bash
belgian-brewery/
├── README.md
├── architecture.png
├── data/
│   ├── kaggle_beer_reviews.csv
│   └── scraped_breweries.csv
├── src/
│   ├── __init__.py
│   ├── ingest/
│   │   ├── __init__.py
│   │   ├── beerscraper.py
│   │   ├── kagaload.py
│   │   └── govapi.py
│   ├── transform/
│   │   ├── __init__.py
│   │   └── scoring.py
│   └── util/
│       ├── __init__.py
│       ├── logger.py
│       └── errors.py
├── notebook/
├── dashboard/
├── requirements.txt
├── .gitignore
└── LICENSE
```

---

## ⚙️ Run the Pipeline

Quick start:

```bash
python3 -m venv venv && source venv/bin/activate    # activate virtual environment
pip install -r requirements.txt                     # install dependencies
python -m src.ingest.beerscraper                    # scrape BeerAdvocate lines
python -m src.ingest.kagaload                       # load Kaggle CSV to BigQuery
python -m src.ingest.beerscraper                    # scrape BeerAdvocate lines
python -m src.ingest.govapi                         # optional enrichment
python -m src.transform.scoring                     # compute partner score model
python -m src.util.logger                           # log results
```
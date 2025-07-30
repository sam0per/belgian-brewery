The goal is not just to analyze beer data, but to treat this project as if you were already the Data Analyst at Glide, supporting their teams.

### Project Plan: The Belgian Beer "Glide Template" Strategy Project

Here is a step-by-step plan. For your video, you will walk them through this process, showing your screen at each stage (your code, your dbt project, your Hex dashboard).

---

### **The Narrative for Your Video**

Start your video by setting a business context that is relevant to Glide. This shows immediate business acumen.

**Your Video Script Opening (Example):**
*"Hi Glide team, my name is [Your Name]. I was really excited to see your Data Analyst role, as it aligns perfectly with my skills in turning data into actionable strategy. To show you how I work, I've put together a project. I'm pretending I'm a Data Analyst at Glide tasked with a new initiative: **'Developing a new premium Glide template for local breweries.'** My goal is to use data to inform the Product, Sales, and Marketing teams on how to build, sell, and market this new template."*

---

### **Project Workflow & How It Maps to Glide's Needs**

This workflow is designed to hit every single point in the job description.

#### **Step 1: Data Infrastructure - Using BigQuery**

*   **Glide's Requirement:** `Strong proficiency with BigQuery and SQL for large-scale data analysis.`
*   **Your Action:**
    1.  Take the Google Sheet you've created (`Belgian beers and breweries`) and upload it as a raw table into Google BigQuery.
    2.  **In your video, show this table in the BigQuery UI.** Say, *"The first step was to establish our data foundation. I've created a workflow in n8n to scrape and transform data from 2 sources:*
        1.  *(List of Belgian beers on Wikipedia)[https://nl.wikipedia.org/wiki/Lijst_van_Belgische_bieren]*
        2.  *(List of Belgian municipalities and provinces on data europa)[https://data.europa.eu/data/datasets/https-www-odwb-be-explore-dataset-communesgemeente-belgium-?locale=en]*
    3.  *I then created a table in Google BigQuery, our company's data warehouse, to handle analysis at scale."*

#### **Step 2: Data Integration & Expansion - Using Python & an API**

*   **Glide's Requirement:** `Data Integration & Expansion: Bring online new data sources (e.g., third-party tools, APIs).` / `Proficiency in Python.` / `Nice-to-Have: Experience integrating external data sources.`
*   **Your Action:**
    1.  Your current dataset has brewery names and provinces, but no specific locations for mapping. You will enrich it.
    2.  Write a simple Python script using libraries like `pandas` and `geopy` (or another geocoding API like Google Maps API) to get the latitude and longitude for each unique brewery.
    3.  Upload this new, enriched location data as a separate table in BigQuery.
    4.  **In your video, briefly show the Python script.** Say, *"To enhance our analytical capabilities, especially for a map-based template, I integrated an external data source. I wrote a Python script to geocode each brewery, giving us the latitude and longitude needed for visualization. This enriched data is now also in BigQuery."*

#### **Step 3: Pipeline & Transformation - Building a dbt Project**

*   **Glide's Requirement:** `Hands-on experience with dbt (data build tool) for modeling and transformation workflows.` / `Build and maintain robust, efficient data pipelines.`
*   **Your Action:**
    1.  Set up a dbt project that connects to your BigQuery instance.
    2.  **Create dbt models** to clean, transform, and structure the raw data into a usable format.
        *   **Staging Models:** Create staging models (`stg_beers.sql`, `stg_breweries.sql`) that perform basic cleaning: casting `abv_pct` to a numeric type, standardizing text, handling nulls.
        *   **Dimension & Fact Models:** Create final, clean tables (`dim_breweries`, `fct_beers`). In `dim_breweries`, you will join the original brewery data with your new geocoded location data. This is a best-practice modeling approach.
        *   **Aggregate Model:** Create a final aggregate table (`agg_province_metrics.sql`) that pre-calculates the number of breweries and beers per province.
    3.  **In your video, show your dbt project structure in your code editor and, most importantly, show the dbt Lineage Graph (DAG).** This is a huge visual win. Say, *"To ensure our data is clean, reliable, and up-to-date, I built a transformation pipeline using dbt. This models our raw data into clean, logical tables—like a dimension for breweries and a fact table for beers. This dbt graph shows the clear, maintainable data flow from source to final analytics models."*

#### **Step 4: Insights & BI - Creating a Hex Dashboard**

*   **Glide's Requirement:** `Design, develop, and maintain KPI dashboards and exploratory analysis in Hex.` / `Experience with Hex (or similar BI tools) for dashboarding, storytelling, and exploratory data analysis.`
*   **Your Action:**
    1.  Connect Hex to your BigQuery project.
    2.  Build a dashboard that answers the strategic questions from your fictional stakeholders. Use SQL to query the clean dbt models you created (not the raw data).

**Your Dashboard should have 3 sections, one for each "stakeholder":**

**1. For the Product Team: "What features should our brewery template have?"**
*   **KPIs:** Total Beers, Total Breweries, Average ABV.
*   **Charts:**
    *   A bar chart of the **Top 15 Most Common Beer Styles**. (Insight: "We should pre-build filters for Tripel, Blond, and Dubbel.")
    *   A histogram showing the **Distribution of Alcohol by Volume (ABV)**. (Insight: "An ABV slider filter would be a powerful feature for users.")

**2. For the Sales Team: "Which regions should we target for outreach?"**
*   **KPIs:** Province with Most Breweries, Province with Highest Average Beer ABV.
*   **Charts:**
    *   **An interactive map of Belgium**, with provinces colored by the number of breweries. Make it clickable so it shows the exact number. (This is a direct answer to their question).
    *   A table of the **Top 5 Provinces by Brewery Count**. (Insight: "Our initial sales outreach should focus on East Flanders, Hainaut, and West Flanders as they have the highest density of potential customers.")

**3. For the Marketing Team: "How can we market this template?"**
*   **KPIs:** Most Prolific Brewery, Beer with the Highest ABV.
*   **Charts:**
    *   A bar chart showing the **Top 10 Breweries by Number of Different Beers Offered**. (Insight: "We can feature breweries like 'Brasserie de l'Abbaye des Rocs' in our marketing campaigns as power users of a potential directory.")
    *   A data-backed "story." For example: "The 'Tripel' style is dominant, accounting for X% of all high-ABV beers. Our launch campaign could be titled 'Build Your Own Belgian Tripel Finder in Minutes.'"

#### **Step 5: The Final Video - Communication & Recommendations**

*   **Glide's Requirement:** `Strong business acumen with the ability to translate data insights into actionable recommendations.` / `Excellent communication and collaboration skills.`
*   **Your Action:**
    1.  Record your screen as you walk through the Hex dashboard.
    2.  For each chart, don't just say what it is. Explain the **"So What?"**. Use the insights you've planned above.
    3.  **Conclude your video** by summarizing your recommendations for the fictional teams and connecting it back to the role.

**Your Video Script Closing (Example):**
*"So, with this analysis, we can provide data-backed recommendations: The **Product team** can prioritize style and ABV filters. The **Sales team** has a clear, targeted list of provinces for outreach. And the **Marketing team** has specific breweries and beer styles to feature in their launch campaign. This project demonstrates my full-stack analytics approach—from pipeline development in Python and dbt to strategic insights in Hex. I'm confident I can bring this same process-driven, results-oriented mindset to help Glide scale its data operations. Thank you for your time."*

By following this plan, you are not just showing a hobby project; you are demonstrating that you can think and operate exactly like the Data Analyst they are looking for. Good luck
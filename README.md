# Video Games Analysis Centered Around Review Ratings, Genre Specification, and Monetization Models
## Team Members
* **Ziyang Liu** | [liuwilly74-cpu](https://github.com/liuwilly74-cpu)
* **Arij Ahmed** | [arijahmed32](https://github.com/arijahmed32)
* **Qi Zhang** | [q11z](https://github.com/q11z)

## Repository
**GitHub:** [2026Spring_projects](https://github.com/liuwilly74-cpu/2026Spring_projects)

---

## Project Overview
**Project Type:** Type III Project (Original Data Analysis)

This research conducts a comprehensive analysis that examines how Steam player ratings, Metacritic scores, monetization structures, and more all relate to one another across two separate datasets. By leveraging historical and contemporary data, we aim to map the evolution of the gaming market and identify the drivers behind commercial success.

### Key Research Questions
1. **Genre Evolution:** Which niche genres have transitioned to the mainstream over the past decade? What do their lifecycle trajectories look like?
2. **Publisher Specialization:** Is there a statistically significant correlation between a publisher’s genre focus and their financial performance?
3. **Indie Breakthroughs:** Through what types of genre innovation (e.g., Roguelikes, Cozy Games) do indie titles achieve market breakthroughs?
4. **Market Distribution:** What is the statistical genre distribution among the Global Top 100 best-selling games across longitudinal trend lines?

---

## Hypotheses
* **Genre Significance:** Certain genres command significantly higher prices than others. For example, RPGs and Strategy games might be priced higher than Casual or Indie games.
* **Monetization Models:** Free to Play (F2P) games with In-App-Purchases (IAP) have a significantly lower Steam positive review rate than F2P games without any IAP. 
---

## Data Sources
The analysis utilizes the following datasets:

1.  **Steam Insights (October 2024):** A comprehensive database including game details, genres, reviews, tags, and SteamSpy insights for over 140,000 games.
    [Source: NewbieIndieGameDev/steam-insights](https://github.com/NewbieIndieGameDev/steam-insights)
2.  **Video Game Sales Dataset:** A dataset analyzing sales data from more than 16,500 games.  
    [Source: Kaggle - Video Game Sales](https://www.kaggle.com/datasets/anandshaw2001/video-game-sales/data)

---

## Methodology
* **Data Cleaning:** Merging Steam metadata with global sales records.
* **Statistical Analysis:** Correlation matrices and variance analysis for sales drivers.
* **Sentiment Analysis:** NLP-based processing of review text to identify trends regarding microtransactions and DLC.
* **Visualization:** Generating longitudinal trend lines and genre distribution charts.

---

## Instruction
* If you want to try the data cleaning process and view the raw dataset quality report, you can execute the file in `Data Cleaning` folder.
* Otherwise you can just execute either jupyter notebook file to view the data analysis.

---

## Citations
* **Claude:** https://claude.ai/share/29fe8991-34c9-49fe-88ec-993e54b5df7e
* **Medium:** https://medium.com/@mattdamberg/data-cleaning-with-python-665930f3215a
* AI assistance on category, tag, genre mapping(Translating foreign languages into standardized english format)
* AI assistance on analysis scope brainstorm. Cursor AI agent was used for this. 

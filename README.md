# Video Game Sales Analysis  

## Team Members
* **Ziyang Liu** | [liuwilly74-cpu](https://github.com/liuwilly74-cpu)
* **Arij Ahmed** | [arijahmed32](https://github.com/arijahmed32)
* **Qi Zhang** | [q11z](https://github.com/q11z)

## Repository
**GitHub:** [2026Spring_projects](https://github.com/liuwilly74-cpu/2026Spring_projects)

---

## Project Overview
**Project Type:** Type III Project (Original Data Analysis)

This research conducts a comprehensive analysis to identify correlations between video game sales and various attributes, including genre, platform synergy, critic-user sentiment, and seasonal release windows. By leveraging historical and contemporary data, we aim to map the evolution of the gaming market and identify the drivers behind commercial success.

### Key Research Questions
1. **Genre Evolution:** Which niche genres have transitioned to the mainstream over the past decade? What do their lifecycle trajectories look like?
2. **Publisher Specialization:** Is there a statistically significant correlation between a publisher’s genre focus and their financial performance?
3. **Sales Variance:** Beyond genre, which factor (Platform, Scores, or Seasonality) best explains sales fluctuations?
4. **Indie Breakthroughs:** Through what types of genre innovation (e.g., Roguelikes, Cozy Games) do indie titles achieve market breakthroughs?
5. **Predictive Keywords:** Which specific "tags" or "keywords" serve as the most reliable leading indicators for a breakout in sales?
6. **Market Distribution:** What is the statistical genre distribution among the Global Top 100 best-selling games across longitudinal trend lines?

---

## Hypotheses
* **DLC Impact:** Games with 5+ paid DLC packs have a statistically lower Steam review score than comparable games in the same genre with no DLC.
* **Microtransaction Sentiment:** Negative sentiment toward microtransactions in reviews increased significantly after November 2017 (following the *Star Wars Battlefront II* controversy).
* **Monetization Models:** Free-to-play (F2P) games with cosmetic-only monetization score comparably to paid games, while "pay-to-win" F2P games score significantly lower.

---

## Data Sources
The analysis utilizes the following datasets:

1.  **Steam Insights (October 2024):** A comprehensive database including game details, genres, reviews, tags, and SteamSpy insights.  
    [Source: NewbieIndieGameDev/steam-insights](https://github.com/NewbieIndieGameDev/steam-insights)
2.  **Video Game Sales Dataset:** A dataset analyzing sales data from more than 16,500 games.  
    [Source: Kaggle - Video Game Sales](https://www.kaggle.com/datasets/anandshaw2001/video-game-sales/data)

---

## Methodology
* **Data Cleaning:** Merging Steam metadata with global sales records.
* **Statistical Analysis:** Correlation matrices and variance analysis for sales drivers.
* **Sentiment Analysis:** NLP-based processing of review text to identify trends regarding microtransactions and DLC.
* **Visualization:** Generating longitudinal trend lines and genre distribution charts.

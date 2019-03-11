<img src="https://theventure.city/wp-content/uploads/2017/06/Theventurecity-logoweb-1.png" >

# Data Pipeline Toolkit for Early-Stage Startups
## **A powerful compilation of free tools and custom code that any startup can adapt to turn raw data into visual insights**

There are lots of reasons why the data your startup is collecting is important: managing your team, impressing investors, delighting customers, planning for the future, etc. But which data? Where do you get it? How do you turn it from raw information into coherent story? And how can you do that on a tight budget? To answer those questions, [TheVentureCity](https://theventure.city/) has developed this toolkit for startup founders who want to supplement their gut and intuition with data-driven insights.

## Make Your Own Copy & Learn by Doing

The purpose of the toolkit is twofold: 
1. To allow any startup to deploy an Extract-Transform-Load (ETL) pipeline that takes raw event log data as its input and feeds visual dashboards as its output. **Our intent is that you will make your own copy of these tools and use them.** If you need our help, just ask.
1. To supply context about what is happening behind the scenes by walking you through the code via Jupyter Python notebooks.

<img src="img/tvc_etl_pipeline_diagram.png" alt="TVC ETL Data Analysis Pipeline" style="width: 100%;"/>

The main engine orchesetrating the ETL pipeline is Python code. It is available in two forms: 
* Notebooks using [Google Colaboratory](https://colab.research.google.com/notebooks/welcome.ipynb)'s cloud Jupyter runtime environment (see below for more notes); and 
* .py files in our GitHub repository

Each notebook contains raw Python code and/or imports our .py files to illustrate a specific type of analysis as listed below. By combining working Python code with a discussion of how it works and why it is important, these notebooks help you learn on your own by analyzing your startup's data. Once the data is extracted and transformed in memory (the "E" and "T" in "ETL"), the Python code loads it into Google Sheets (the "L" step). From there, Google Data Studio connects to Google Sheets to enable visualization and dissemination of the transformed data.

Even if you do not have a dedicated data analyst at this time—and most of early-stage teams of 5-10 members do not have that role—make sure there is somebody on your engineering team who is tasked with instrumentation and analysis. It is this person who most needs to review this toolkit. The Python code is fully commented and ready to run as-is in the cloud notebook environment. When you have adapted it to your business and are ready to automate your pipeline, you'll need to convert the notebooks to .py files scheduled to run at regular intervals.

Be sure to bookmark this page so you can stay up-to-date as we continue to deploy new features.

## Analysis Menu

Follow the links below to access the Jupyter notebook for that topic. Feel free to jump around as you see fit, though Items 0 (Introduction to Notebooks) and 1 (Data Building Blocks) should not be missed.

**0. Introduction to Notebooks**

* [Google Colaboratory Basics](https://colab.research.google.com/notebooks/welcome.ipynb)
* [Jupyter Notebook Quickstart](https://jupyter.readthedocs.io/en/latest/content-quickstart.html)

**1. Data Building Blocks** — *START HERE!* Before you can start analyzing the data, you need to understand raw event log data and how to access it. Then the raw data needs some pre-processing to convert it into a “DAU Decorated” data set, which serves as the jumping-off point for the rest of the analysis. 

* Part 1: Understanding Event Logs
* Part 2: Create the “DAU Decorated” data set

**2. Engagement** metrics gauge the extent to which users find value in the product by measuring the frequency with which they use it. In this way, we can use data to assess and track product-market fit, an important but tricky concept for which data. Solid engagement sets the stage for retention over a long period of time.

* Part 1: DAU Histogram and DAU/MAU Ratio 
* Part 2: DAU Histogram and DAU/MAU Ratio Trends over Time
* Further reading: Our post on [Going Beyond DAU/MAU Metrics for Growth](https://medium.com/theventurecity/going-beyond-dau-mau-metrics-for-growth-169b9eac7aec)

**3. Retention** metrics measure how long users continue to use the product after the first time they use it. Good retention makes growth so much easier and efficient: newly-acquired users count toward user growth rather than merely replacing lost users. 

* Part 1: Cohort User Retention
* Part 2: Month-over-Month and Week-over-Week Retention

**4. Growth Accounting** combines user or revenue growth with retention to arrive at a measure for growth efficiency called the Quick Ratio. Be sure to check out our post introducing this concept, [Quick Ratio as a Shortcut to Understand Product Growth](https://medium.com/theventurecity/quick-ratio-as-a-shortcut-to-understand-product-growth-ae60212bd371). 

* Part 1: User Quick Ratios
* Part 2: Revenue Quick Ratios
* Part 3: Rolling Quick Ratios
* Part 4: Segmented Quick Ratios
* Further reading: Our post on [Rolling Quick Ratios for Product Decision-Making](https://medium.com/theventurecity/rolling-quick-ratios-for-product-decision-making-ec758166a10f)

**5. Cohort Analysis** helps to measure not only user retention, but also customer long-term value (CLTV).

* Part 1: Cumulative Cohort CLTV
* Part 2: Cumulative Cohort CLTV Trends
* Part 3: Segmented Cohort Analysis

**6. Visual Dashboards** help make a store and point 

* Part 1: Integrating Python with Google Sheets
* Part 2: Google Data Studio visualizations from Google Sheets

<img src="img/Meme-DA.jpg" alt="Isn't data analytics just analyzing data?" style="width: 400px;"/>

## Credits

* This toolkit builds upon the fantastic [work](https://medium.com/swlh/diligence-at-social-capital-part-1-accounting-for-user-growth-4a8a449fddfc) done by Jonathan Hsu and his team at Social Capital
* Thanks to [Analytics Vidya](https://www.digitalvidya.com/blog/reasons-data-analytics-important/) for the meme.

## Notes

With the Google Colaboratory option there is no need to install any software. The other option is to install [Jupyter](https://jupyter.org/) (Python 3.6) and the relevant libraries on your local machine. Whatever your comfort level with Python, we encourage you learn by doing: hit Shift-Enter to run each cell and see what happens. If you want some exposure to some Python basics to supplement this toolkit, we recommend [DataCamp](https://www.datacamp.com/), which has some excellent free courses, including a [tutorial on Jupyter notebooks](https://www.datacamp.com/community/tutorials/tutorial-jupyter-notebook). Where possible, we are including sample Excel spreadsheets; however, most of the time, Python is the superior tool because it can handle larger data sets more easily and perform loops.
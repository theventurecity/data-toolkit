<img src="https://theventure.city/wp-content/uploads/2017/06/Theventurecity-logoweb-1.png" >

# Data Pipeline Toolkit for Early-Stage Startups
## A powerful compilation of free tools and custom code that any startup can adapt to turn raw data into visual insights

There are lots of reasons why the data your startup is collecting is important: managing your team, impressing investors, delighting customers, planning for the future, etc. But which data? Where do you get it? How do you turn it from raw information into coherent story? And how can you do that on a tight budget? To answer those questions, [TheVentureCity](https://theventure.city/) has developed this toolkit for startup founders who want to supplement their gut and intuition with data-driven insights.

## Make Your Own Copy & Learn by Doing

The purpose of the toolkit is twofold: 
1. To allow any startup to deploy an Extract-Transform-Load (ETL) pipeline that takes raw event log data as its input and feeds visual dashboards as its output. **Our intent is that you will make your own copy of these tools and use them.** If you need our help, just ask.
1. To supply context about what is happening behind the scenes by walking you through the code via Jupyter Python notebooks.

<img src="img/tvc_etl_pipeline_diagram.png" alt="TVC ETL Data Analysis Pipeline" style="width: 100%;"/>

The main engine orchestrating the ETL pipeline is Python code. It is available in two forms: 
* Notebooks using [Google Colaboratory](https://colab.research.google.com/notebooks/welcome.ipynb)'s cloud Jupyter runtime environment (see below for more notes); and 
* .py files in our GitHub repository

Each notebook contains raw Python code and/or imports our .py files to illustrate a specific type of analysis as listed below. By combining working Python code with a discussion of how it works and why it is important, these notebooks help you learn on your own by analyzing your startup's data. Once the data is extracted and transformed in memory (the "E" and "T" in "ETL"), the Python code loads it into [Google Sheets](https://docs.google.com/spreadsheets/d/1-XnO_eWkRwX-E1fiA2Jkbe3kJvoyoPFsdeW7vnF6zS0/edit#gid=0) (the "L" step). From there, [Google Data Studio](https://datastudio.google.com/reporting/1xjS__Q6ZUXuUUARkgRvY4spYUw1ePksV/page/ctyj) connects to Google Sheets to enable visualization and dissemination of the transformed data.

Even if you do not have a dedicated data analyst at this time—and most of early-stage teams of 5-10 members don't have that role—make sure there is somebody on your engineering team who is tasked with instrumentation and analysis. It is this person who most needs to review this toolkit. The Python code is fully commented and ready to run as-is in the cloud notebook environment. When you have adapted it to your business and are ready to automate your pipeline, you'll need to convert the notebooks to .py files scheduled to run at regular intervals.

Be sure to bookmark this page so you can stay up-to-date as we continue to deploy new features.

## Analysis Menu

**0. Introductions to Notebooks & Google Tools**

* [Google Colaboratory Basics](https://colab.research.google.com/notebooks/welcome.ipynb) (a.k.a. "Colab")
* [Jupyter Notebook Quickstart](https://jupyter.readthedocs.io/en/latest/content-quickstart.html)
* [Google Sheets @ G Suite Learning Center](https://gsuite.google.com/learning-center/products/sheets/get-started/#!/)
* [Google Data Studio Help](https://support.google.com/datastudio/answer/6283323?hl=en)

**1. Data Analysis Building Blocks** — Before you can start analyzing the data, you need to understand raw event log data and how to access it. Then the raw data needs some pre-processing to convert it into a “DAU Decorated” data set, which serves as the jumping-off point for the rest of the analysis. Inspecting the Google Sheets and Google Data Studio pieces of the puzzle will help you understand these critical components as well.

* Understanding Event Logs ([GitHub](Understanding_Event_Logs.ipynb) | [Colab](https://colab.research.google.com/drive/1GiaZdWy3PDevWYLolFfGgb9Bp_7Yjvgv))
* Create the “DAU Decorated” data set ([GitHub](Create_the_DAU_Decorated_Data_Set.ipynb) | [Colab](https://colab.research.google.com/drive/12uehG2EcIqxcTazKs-pNQRTQSckllOmE))
* Explore the [Google Sheets workbook](https://docs.google.com/spreadsheets/d/1-XnO_eWkRwX-E1fiA2Jkbe3kJvoyoPFsdeW7vnF6zS0/edit#gid=0) these pipelines use to store the data after it gets transformed--the "Load" step. It is read-only. Therefore, to use this pipeline on your own, you need to create your own copy of this workbook to your Google Drive account. 
* Explore the [Google Data Studio](https://datastudio.google.com/reporting/1xjS__Q6ZUXuUUARkgRvY4spYUw1ePksV/page/ctyj) that reads from the Google Sheets workbook to create the visualizations. It is also read-only, so create your own copy under your Google Drive account.

**2. Mini-Pipeline** notebooks are stand-alone, "full stack" pipelines designed to teach the specifics of a particular subset of startup data analytics, carrying out each step of the Extract-Transform-Load-Visualize process along the way. In particular, each "Transform" step contains verbose, commented code and an explanation of the data transformation taking place. We suggest you review these Mini-Pipelines first before trying to implement the Full Pipeline below. 

**Note**: the embedded iFrame visualizations from Google Data Studio do not render in the GitHub version of the notebooks. To see the visuals, visit the Colab version of the notebook or visit the [Google Data Studio dashboard](https://datastudio.google.com/reporting/1xjS__Q6ZUXuUUARkgRvY4spYUw1ePksV/page/ctyj).

* The **Mini-Pipeline: MAU Growth Accounting** notebook ([GitHub](Mini_Pipeline_MAU_Growth_Accounting.ipynb) | [Colab](https://colab.research.google.com/drive/1moHa4Mcycwsz7Fq6T_5Zou1Zunt0afiI)) aggregates DAU Decorated at a monthly level, categorizes different types of users in each month, and then uses that information to arrive at a measure for growth efficiency called the Quick Ratio. Be sure to check out our post introducing this concept, [Quick Ratio as a Shortcut to Understand Product Growth](https://medium.com/theventurecity/quick-ratio-as-a-shortcut-to-understand-product-growth-ae60212bd371). 

    * Further reading: Our post on [Rolling Quick Ratios for Product Decision-Making](https://medium.com/theventurecity/rolling-quick-ratios-for-product-decision-making-ec758166a10f)

* The **Mini-Pipeline: Cohort Analysis** notebook ([GitHub](Mini_Pipeline_Cohort_Analysis.ipynb) | [Colab](https://colab.research.google.com/drive/1oYy-wJl6VZFgOsv8uw7iGChQxUjrR5rf)) transforms the DAU Decorated data set into a cohort analysis dataframe to examine monthly user retention and cohort revenue LTV. Cohort *retention* metrics help us see how long users continue to use the product after the first time they use it. Good retention makes growth so much easier and efficient: newly-acquired users count toward user growth rather than having to replace lost users. 

* The **Mini-Pipeline: Engagement** notebook ([GitHub](Mini_Pipeline_Engagement.ipynb) | [Colab](https://colab.research.google.com/drive/1nznm8WRU0dJcMNAR4U5CpkHpbMI-nmWD)) shows how to transform the DAU Decorated data set into engagement dataframes to analyze a DAU Histogram, Active Days per Month over Time, and Multi-Day Users Ratio over Time. *Engagement* metrics gauge the extent to which users find value in the product by measuring the frequency with which they use it. In this way, we can use data to assess and track product-market fit, an important but tricky concept for which data helps supplement gut feel. Solid engagement sets the stage for retention over a long period of time.

    * Further reading: Our post on [Going Beyond DAU/MAU Metrics for Growth](https://medium.com/theventurecity/going-beyond-dau-mau-metrics-for-growth-169b9eac7aec)

**3. Full Pipeline** -- This notebook ([GitHub]() | [Colab]()) combines the logic from each of the mini-pipelines into one. Instead of using verbose, inline code, it leverages TheVentureCity's python libraries to perform the Transform step. If you want to run a complete data pipeline for your business:
1. Create a copy of this notebook in your Google Drive account
1. Configure it to point to...
    1. Your raw data source
    1. Google Sheets workbook
    1. Google Data Studio
1. Configure Google Data Studio to point to the Google Sheets workbook
1. ...and give it a try!

<img src="img/Meme-DA.jpg" alt="Isn't data analytics just analyzing data?" style="width: 400px;"/>

## Credits

* This toolkit builds upon the fantastic [work](https://medium.com/swlh/diligence-at-social-capital-part-1-accounting-for-user-growth-4a8a449fddfc) done by Jonathan Hsu and his team at Social Capital
* Thanks to [Analytics Vidya](https://www.digitalvidya.com/blog/reasons-data-analytics-important/) for the meme.

## Notes

With the Google Colaboratory option there is no need to install any software. The other option is to install [Jupyter](https://jupyter.org/) (Python 3.6) and the relevant libraries on your local machine. Whatever your comfort level with Python, we encourage you learn by doing: hit Shift-Enter to run each cell and see what happens. If you want some exposure to some Python basics to supplement this toolkit, we recommend [DataCamp](https://www.datacamp.com/), which has some excellent free courses, including a [tutorial on Jupyter notebooks](https://www.datacamp.com/community/tutorials/tutorial-jupyter-notebook). 
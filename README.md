-----www.krisha.kz scrapper-----

This project's main goal is to implement webscrapping on www.krisha.kz and additionally
conduct simple analytics on Almaty rent market using DataBricks. 

Raw data includes data on price, district, address, floor etc. 
for each current post that fulfills the following conditions:
- Includes pictures.
- Not the first floor.

Tools used for the project include: DataBricks, BeautifulSoup, Azure ADLS Gen2, Power BI.

Key stages:
- Scrape raw data related to long-term apartment rent from Krisha.kz using BeautifulSoup+lxml.
- Clean data inside the scrapper.
- Upload data to ADLS Gen2 container.
- Ingest raw data to DataBricks.
- Transform and enrich data using PySpark.
- Partition and upload processed data to "processed tier" blob in ADLS Gen2.
- Connect Power BI to the container and create a sample dashboard.

The final application can be used for webscrapping relevant data from www.krisha.kz in any city.

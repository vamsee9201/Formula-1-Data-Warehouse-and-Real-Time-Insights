# Formula 1 Data Warehousing and Real Time Insights

![Process](Docs/Flowchart.jpeg)

Data is extracted from flat files and Ergast API, then staged and transformed in Python for further processing. Subsequently, this data is loaded into a BigQuery data warehouse. This entire workflow is managed and orchestrated in Airflow. Once the data is prepared, it can be utilized for visualization in Tableau and for deriving insights.

### <ins>The data warehouse schema is structured as follows:</ins>

![Process](Docs/schema.jpeg)

### <ins>Visualizations:<ins>
### Driver's performance over the years:
link : https://lookerstudio.google.com/u/0/reporting/cd331a78-3319-4993-a538-2281e6502f4c/page/z6qfD
![Process](Docs/driverPerformance.jpg)
### <ins>Planned visualizations:</ins>
- Race results dashboard
- Driver performance trends dashboard
- Driver comparison dashboard
- Team performance trends dashboard
- Pitstop and race results comparison dashboard
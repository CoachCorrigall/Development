# Development

This is a repository that shows some of the work I have developed over the last year. 
As of the end of March, I have two power BI files and a three SQL files for review.

**UPDATE (04/02/2024):**
Adding zip files that show pipelines I have created in the past for data movement jobs in Azure sysnapse/Azure Data Factory. Most of these files can only be exported as templates inside the Azure tool and will need to have linked services setup to work as expected. 

## Power BI Reports
(1) The NBA reporting package is a project I have developed in my free time. This report allows me to bring together two things I love in life - basketball and data.

(2) The building Division Dashboard is a report that I have developed for a client over the five months. The data has been anonymized and condensed. Due to time, effort, and a focus on anonymity, some of the data may appear slightly strange (i.e. budgets would consider the entire suite of BUs, not just the ones I have selected), but the report can be used to analyze and understand my ability to write DAX Measures. I would be happy to explain any aspect of the report in further detail.

## SQL Statements
(1) The account balance SQL statement is the primary view populating the building division dashboard. During my tenure as a consultant/analyst/developer/data person, I have worked with the backend (data modeling  + data warehousing) and frontend (report development) tools needed to meet the client's business needs. I enjoy owning a solution from beginning to end. Having visibility into both aspects allows a developer to implement logic at the esaiest level for repeatability and performance. When developing a solution, it is crucial to understand which logic should be applied at which layer of the ETL process. Due to the time intelligence required for reporting, there was a considerable amount of thought placed into where certain logic exists, both in the warehouse and in the report itself.

(2) The "build SQL source statement" code was a collaborative project to support the initiative of loading metadata from a SQL source into a target location (an azure data lake or azure SQL database) in a more systematic and repeatable way. Most of the underlying metadata tables in the code are streamlined "create table" statements that mimic the sys.<insert_table> columns from a SQL server source.

(3) The "create_f_ns_SalesOrderLine" SQL statement shows a typical way that I would handle merges from a lower layer of the warehouse into a more refined layer for reporting purposes.

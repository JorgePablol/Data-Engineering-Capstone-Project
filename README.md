# z3

The project scope is to solve a problem within my organization, we scrape data automatically via scrapper/robots from websites to our clients (I may not be very explicit, I don't want to get sued), the problem is that sometimes the actual data quality checks fail, and often they do because the indicators given by the site are literally duplicated for example a product that has sold 50 units it happens to have 100, I mean the website returns it duplicated, another problem is that sometimes the websites return extremely little values, instead of 100 a product returns 1 or even 0 as their sales for example.

In this project I will solve 3 of my most important problems right now first two were described above and the third is to finally finish my capstone project, the scope is to extract the indicators uploaded on each client database, process those values and return what days can possibly have extreme values, upload that result into a database that will let everyone know what days of which client and provider they have to regenerate (regeneration means deleting the data and scraping it again) in order to update for more accurate values.

## Project folders/files
* base/:
    * **constants.py**
    * **z3_base.py**: The whole project base, with all the etl steps implemented.
    * **z3_interface.py**: A simple interface that defines the base methods z3 will have.
* engineering/:
    * **engineering.py**: A collection of functions that prepare the data to be processed.
* extract_and_quality/:
    * **extract_and_quality_queries.py**: Client database queries for each report type, and the data quality query.
* load/:
    * **load_queries.py**: SQL statements needed to create tables, insert records and drop tables.
* z3_nodes/:
    * **z3_inventory.py / z3_sellout.py**: An object that inherits the z3_base, and sets up specific paremeters for each type of report.
* results_output/:
    * **results_inventory.csv/results_sellout.csv**: The actual final results queried from the z3_results database.
* **z3.ipynb**: The jupyter notebook where you can execute the etl.
* **data_dictionary.xlsx**: Is there a more explicit name?
* **raw_data_INVENTORY.csv**: The raw data extracted from the clients database.
* **raw_data_SELLOUT.csv**: The raw data extracted from the clients database.
* **.env**: All the key and secrets needed to access the data sources, and other top secret information like clients and providers.


## Run the program
1. To run the project simply run the import cell on the jupyter lab.
2. Run the cells that initialize the nodes z3Sellout() for example.

## Step 1: Scope the project and gather the data
I will use the data from our clients databases,  I will anonymize an example of the raw data extracted from the databases, the data sources which are each database are more than 2 as it's expected in the project rubric. As it's defined in the anonymized datasets, the raw data extracted from the database surpasses 5 million rows on each report type (sellout, inventory). From those rows it summarizes the indicators pos qty, pos sales (sellout) and curr on hand qty (inventory) and groups it into the report daily (the date to which the data corresponds).


**Correction**
The base tables sellout and inventory interact with 4 different tables, the first one is the historical_execution_results that let us improve the query performance, that one is used in the query. Helps me to know what execution ids I must filter by id instead of filtering the sellout by a date range in this case the comparation would be against millions of rows.

The second one is the config reports table that is loaded as a variable for each client, this variable is a copy from the config report table of scrapper database, with only 2 important values first the provider as the key and second the config report id, the config report id let us know what specific report are we talking about, that specific report_id is combined into the extract query, and having that file in local instead of performing another query allows us to save some connections, let's say our database instances usually carry 20 out of 10 connections, what I mean is that they are always overwhelmed. Here's an example:

CLIENT_1="{
    'SELLOUT':{
        'PROVIDER': 18,
        'PROVIDER: 17,
        'PROVIDER': 13,
        'PROVIDERY': 14
    }"


The other two important tables are hidden into the .env file, there I show the client name and client_id, provider name and provider_id, to correlate the provider name, and client name into the actual report that is being queried, the value of the dictionary assigns an id for each provider/client. So I know to which client and provider each report belongs to.

That combination happens in the extract function in z3_base.py file.

Example of the tables/dictionaries:
 
PROVIDER_IDS="{
    'PROVIDER': 1,
    'PROVIDER': 2,
    'PROVIDER': 3,
    'PROVIDER': 4,
    'PROVIDER': 5,


CLIENT_IDS="{
    'CLIENT': 1,
    'CLIENT': 2,
    'CLIENT': 3,
    'CLIENT': 4,
    'CLIENT': 5,
    'CLIENT': 6,
    'CLIENT': 7
}"


## Step 2: Explore and asses the data
As I said in the project scope the two most important problems for us are when indicators have an extremely high value (duplicated) or extremely low values, I will solve this by taking the outlier limits that are calculated by substracting inter quartile range * 1.5 to quartile 1 or adding it to quartile 3, but instead I will use inter quartil range * 3, because I dont need to find data variability outliers instead only extreme ones.

## Step 3: Define the data model
#### Conceptual model
My data model will be a star schema, since we need to analyze specifically numeric indicators, that will tell us what's wrong with the data gathered by the robots, it will be easier for joins and centered on the quantitative indicators.


### Database tables
#### Fact Table

* **indicators** - quantitative values that show the performance of a product.
    * client_id, report_id, provider_id, execution_id, daily_id, scrapper_pos_qty, scrapper_pos_sales, scrapper_curr_on_hand_qty, scrapper_rows.

#### Dimension Tables

* **clients** - Our clients.
    * client_id, client
* **daily** - The date to which the data belongs.
    * daily_id, daily, daily_year, daily_month, daily_day.
* **reports** - The report type of the data uploaded.
    * report_id, report_type.
* **providers** - The store type/brand where the products are being sold. 
    * provider_id, provider.
* **executions** - The identifier of the data that is loaded into the z3_results database once you run any of the nodes (sellout or inventory).
    * execution_id, execution_date, execution_year, execution_month, execution_day, execution_hour, execution_minute.

#### Mapping out data pipelines
* 1. Summarize the data in the database of each client, grouping the results by daily, and saving the columns provider, client, pos qty, pos sales and curr on hand qty columns.
* 2. Process the past into a dataframe and apply statistical rules described on step 2 to filter the values we want.
* 3. Take the result into a master of all clients and providers, and posible days with extreme values, create the id of each of the dimension tables.
* 4. Slice the master dataframe into the star schema tables.
* 5. Perform the loading queries for each table.
* 6. Run the tests to confirm the data loaded has the same values as the data extracted and processed, I do this with 5 tests, each one correlates with the 3 principal indicators, also the scrapper rows that correspond to the original report uploaded into the clients database, and the total dailys in the results of step 2 must be present in the dailys uploaded that correlate with the execution id.
* 7. Apply the zzz, and sleep better, you will be aware if the websites are returning useless data, and the clients will be happy for it.
* 8. If needed drop the tables by running the drop_tables method on jupyter lab.

## Step 4: Run pipelines to model the data
The pipelines are defined in the **base/z3_base.py** file, the most important methods that let us execute the pipelines are **extract_and_transform_each_provider_and_client()** it's too clear what it does haha! **load()** that takes the results into star schema and uploads into the z3_results database, **data_quality_checks()** that performs tests and **drop_tables()** that gets rid of the tables if needed. 

The data dictionary is in a separate file.

## Step 5: Complete project write up
#### Whats' the goal?
The goal is to make sure data makes sense once it is into the scrappers database, since that data is going into the clients BI side.

### How would Spark or Airflow be incorporated?
The project is kind of thinked to be taken into an airflow dag, each of the steps is a clearly defined action that can be re-coded into python operators, spark will be implemented in case the data amount grows exponentially, at this point after summarizing the results the cost of processing is low, so that's the only way that implementing spark would be worth doing.


#### Choice of technologies
* Database: Postgresql as the open source, user friendly technology and big data capabilites once the data amount grows.
* Data wrangling: Pandas, one of the best libraries to manipulate and analyze dataframes, I also have plenty of experience with pandas.
* User Interface: Jupyter lab: As a easy to use python interface that let's you run the code with a simple click.

#### Data updates
The data must be updating everyday at 9:30 am, in order to spot useless data and have time to regenerate the reports before the client sees it on the Bussiness Intelligence report.

#### Scenarios
* Data was increased to 100x: In this case the option can be to implement Spark into the project to run the etl on parallel with EMR instances. Other solution would be to add more processing units to each database in order to accomplish the extract query, probably the other step would be to separate each client into its z3 database, since I am putting all the data togheter into one database. 
* 7am update: Get the project into an airflow DAG, so nobody will have to execute the jupyter lab manually.
* If the database needed to be accessed by 100+ people: Implement a distributed database in order to have high availability.
Another solution is to increase the database hardware, another way to solve this depending on the needs is to implement views to 'pre-load' the most complex queries.





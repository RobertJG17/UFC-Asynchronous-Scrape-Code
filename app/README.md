# This is a simple setup for creating a local database containing UFC Fighter Data

## This scraping implementation was done synchronously but has adapted to an asynchronous implementation to meet the 
## demands of the project.
Here are some interesting metrics in our sync v. async approach:
* Time to run in jupyter notebook synchronously ~ 5522 seconds | 90 mins | 1.5 hrs
* Time to run in .py script leveraging asyncio and aiohttp ~ 185.19 seconds | 3 mins | 0.05 hr
* Overall, the async execution time proved to be 96% faster and more effective for fetching a large volume of endpoints

### **docker-compose.yaml**:
* Provisions a pgadmin container and a pgdatabase container.

### **fighter-scrape.py | fighter-data-scrape.py**:
* Running these files allocate the necessary csv files to read in for the migration.

### **migration-postgres.py**:
* Running this exports csv files to postgres databases hosted in docker container.
* Volumes for pgadmin and postgres database are stored in source. 
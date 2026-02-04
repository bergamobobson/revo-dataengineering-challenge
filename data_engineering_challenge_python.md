# Data Engineering challenge

## Description
The file "84710ENG_UntypedDataSet_04082023_113601.csv" is a dataset about trips performed in the Netherlands from 2018 to 2022 for various needs and purposes.  
Each row contains some statistics (number of trips in a year, travelled kilometers in a year, hours travelled in a year) and details (travel motives, type of individual, mode of travel, region, period) about the trips of a single individual in a year.  
The other file “84710ENG_metadata.csv” contains a semi-structured description of the main dataset and of its dimensions (including domains).

## Tasks

### First task
**Design** a simple relational database using PostgreSQL (or any other relational database system of your choice) to store the dataset.  
You can use a local DBMS installation for development, but keep in mind that the target database will be hosted on a Cloud IaaS RDBMS (e.g., AWS RDS for PostgreSQL).

### Second task
**Develop** a simple ETL pipeline loading the data in your database using Python or PySpark.  
Bonus points for deploying your pipeline as a Docker container; you can use a local Docker installation to simulate a cloud deployment.  
The “Extraction” component will be mainly responsible for extracting dimensions from the metadata.  
The “Transform” component will be mainly responsible for data validation and cleansing, as appropriate.  
The “Load” component will be minimal: just load the data in the database according to relational consistency, giving due consideration to idempotency issues.

### Third task
**Query** your database to answer as closely as possible the following questions:
- Total number of trips across all the years, grouped by travel method and by level of urbanization for people who went shopping for groceries
- Top 10 users based in west Netherlands, who travelled the most by bike (in terms of km) to go to work
- Among the top 8 users who travel the most km by bike, show the 3 least common reasons for travelling in year 2022
- Among the top 10 users who spend the least number of hours travelling to attend education/courses, show for every year the average number of trips made by these users.

### Fourth task
Make your delivery production ready!  
**Structure** your code (modules, classes and methods) for clarity and maintainability, write unit tests, optimize for robustness and performance, etc.

## Discussion
We will be discussing your deliverables in a short live session organized as follows: 
1. Live demo of your pipeline and queries (on your local machine) – 5 min
2. Code review – 15 min
3. Data Engineering considerations beyond these simple tasks:  
	I am interested in your approach to – non exhaustive examples - software life cycle management, software quality and testing, security, evolvability, scalability, data quality, data governance, data observability, cost efficiency, etc.  
	E.g., what happens when some future updates to the dataset will exhibit additional attributes?  
	When one or more dimensions will change?  
	When the data volume will grow x100?  
	You may support your discussion with slides, but it’s not a requirement (life is too short for PowerPoint) – 30 min.

Thanks for your effort, have fun and good luck!

John, Andrea, Davide
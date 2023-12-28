# Portfolio Project

Intro: 
* Hello my name is Jon Hack. I am a Data Engineer from British Columbia, Canada. I am passionate learner within the spaces of Data Engineering & Data Analytics. This is my portfolio space to demonstrate my skills, and experience within the spaces of Data Engineering and Data Analysis. 
* This project is 100% for non-commercial use, and is used for demonstration purposes only. This portfolio project will focus on data from a popular North American Sport, baseball, with the statistics focusing on the Major League Baseball League 2022 season.
* This portfolio project is in ongoing development as I have free time to dedicate to its development. 

The topics I will cover through building out this project are: 
* Extract, Transfer, Loading (ETL) Development
* ETL orchastration
* Data Cleansing
* Data Modelling
* Data Visualization
* Data Quality Validation
* Data Quality Testing
* Continuous Integration 

The tech stack used within this Project is as follows: 
* Python
    * Libraries in use:
        * Pandas
        * Unittest
        * Numpy
        * OS
        * Pathlib
* SQL
    * Currently, I am writing data to PostgreSQL tables as intermediate steps within my DAGs. This data is then read from these same tables, to be transformed and further deposited in a secondary PostgreSQL table or final CSV destination. These intermediate SQL tables are dropped at the end of the DAGs. 
* Docker
    * Implemented Airflow through use of Docker Desktop. 
* Airflow 
    * This project utilizes Airflow, which I have running using a Docker Container. All of my ETLs are orchastrated using Airflow, due to its ease of breaking DAGs into smaller sequences of work. This assists with identifying the root cause of errors more easily. 
* GIT
    * This GitHub repo is where I will display my background with basic GIT commands. I have also used this service to implement a CI pipeline, listed further below.
* Dashboarding Tool - Power BI
    * Power BI is being used as the front end reporting layer of this project. This is where I will build out different visuals based on the facts and dimensions built for this project. 
    * The UI UX has been developed using Figma, to assist with organizing the numerous reporting assets used from Power BI.
* Data Quality Testing & Functional Testing
    * Unit Testing - Python unittest Library
        * I will be writing unittests to validate that the transformed data at different stages of the project aligns with known conditions, and the expected outputs of certain functions.
        * The tests being written fall into a few different categories: 
            Test Plans based on the schema of each object.
            1. Duplication Testing 
            2. Test column Set
            3. Null Testing 
            4. Variable Type Testing
            ACTION - TO DO
            5. Aggregated Testing
            6. Logic-based Testing
            7. Illegal Column Values Testing
            8. Accepted Value Testing
            9. Refrential Integrity Testing
            ACTION - TO DO
            10. Multi-data source testing ---> Compare against additional reference
* CI/CD
    * I will be setting up a basic CI pipeline to ensure that following any changes to the project that the corresponding data is properly tested. 
    * ACTION - continue to build upon this pipeline. 
* Python Data Visualization 
    * ACTION - matplotlib
    * ACTION - Seaborn
    * ACTION put these into Power BI 

* Data Model
    * Dimensions:
        * Team Rosters
        * Team Stats (ACTION ---> Change to a FACT?)
        * Dates
    * Facts:
        * Events
        * Gamelogs

* References:
    * Events
        * Link to Year by Year Events - https://www.retrosheet.org/game.htm
        * Detailed Description of Events File - https://www.retrosheet.org/eventfile.htm
    * Gamelogs
        * Link to Year by Year Gamelogs - https://www.retrosheet.org/gamelogs/index.html
        * Column order of the Gamelogs file - https://www.retrosheet.org/gamelogs/glfields.txt
    * Secondary Data Source (Used for comparison to validate my summarized information)
        * Link: https://www.rotowire.com/baseball/stats.php?season=2022
        
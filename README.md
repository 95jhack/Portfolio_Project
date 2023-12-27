# Portfolio Project

Intro: 
* Hello my name is Jon Hack. I am a Data Engineer from British Columbia, Canada. I am passionate learner within the space of Data Engineering. This is my portfolio space to demonstrate my skills, and experience within the spaces of Data Engineering and Data Analysis. This project is 100% for non-commercial use, and is used for demonstration purposes only. This portfolio project will focus on data from a popular North American Sport, baseball, with the statistics focusing on the Major League Baseball League 2022 season.

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
    * Currently, I will be writing data to PostgreSQL tables as intermediate steps within my DAGs
* Docker
    * Implemented Airflow through use of Docker Desktop. 
* Airflow 
    * As of 2023/12/13 - I have moved the project to use Airflow, which I have running using a Docker Container. I will be expanding the structuring of my current DAGs over time to continue to display my skills in this area.
* GIT
    * I will be using this GitHub repo, and will be using my background with GIT to display my understanding.
* Dashboarding Tool - Power BI
    * Reporting File --> See presentation files directory
    * UI UX developed using Figma 
* Data Quality Testing & Functional Testing
    * Unit Testing - Python unittest Library
        * I will be writing unittests to validate that the transformed data at different stages of the project aligns with known conditions, and the expected outputs of certain functions.
* CI/CD
    * I will be setting up a basic CI pipeline to ensure that following any changes to the project that the corresponding data is properly tested. 
    * ACTION - continue to build upon this pipeline. 
* Postman / Use of APIs
    * ACTION - TBD
* Python Data Visualization 
    * ACTION - matplotlib
    * ACTION - Seaborn

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

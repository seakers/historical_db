# Historical Database back-end and skill

## Installation instructions

1. Clone the repository using git or download a copy of the last commit as a zip folder (https://help.github.com/articles/cloning-a-repository/)

2. Install Python 3 on the system if not yet on it (https://www.python.org/downloads/ or use a packet manager if in Linux)

3. Install the latest PostgreSQL on the system if not yet on it (https://www.postgresql.org/download/ or use a packet manager if in Linux)

4. Create a python 3 virtual environment inside the cloned folder (http://python-guide-pt-br.readthedocs.io/en/latest/dev/virtualenvs/)

5. Install all python requirements by executing `pip install -r requirements.txt` while inside the virtual environment (look at the link in 4 to see how to enter the virtual environment)

6. Installation done!

## Database scraper

1. While in a console, change directories to the scraper/ one

2. Run `scrapy crawl ceosdb_scraper` with the $USER and $PASSWORD for PostgreSQL defined as environmental variables of the system

3. Wait for a few minutes for the database to be scraped.

4. Done!

## Machine learning

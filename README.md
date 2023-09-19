# Snowflake Data Ingestion
This repository contains Python code for ingesting data into Snowflake using the Snowflake Connector. The code performs the following tasks:

Create or replace staging tables for various types of raw data.
Cleans and transforms the data before inserting it into Snowflake tables.
Uses Snowflake's SQL syntax for table creation and data insertion.

## Prerequisites
Before running this code, make sure you have the following prerequisites in place:

1. Python 3.x installed on your system.
2. Snowflake account credentials stored as environment variables:

- USER: Your Snowflake username.

- PASSWORD: Your Snowflake password.

- ACCOUNT: Your Snowflake account URL.

- ROLE: Your Snowflake role.

## Installation
Clone this repository to your local machine:

```bash
git clone https://github.com/krissemmy/DFA23-DATATHON.git
```
Install the required Python libraries using pip:

```bash
pip install snowflake-connector-python python-dotenv
```
## Usage
Replace the placeholders in the .env file with your Snowflake account credentials.

Run the Python script:

```bash
python snowflake_data_ingest.py
```
This script will execute the SQL queries defined in the query_1 list to create staging tables and insert cleaned data into Snowflake.

## Error Handling
If any errors occur during data insertion, the script will print an error message and roll back the transaction to ensure data integrity.

## Contributing
Contributions are welcome! If you find any issues or have improvements to suggest, please open an issue or create a pull request.

## License
This project is licensed under the MIT License - see the LICENSE file for details.


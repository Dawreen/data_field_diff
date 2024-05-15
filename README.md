# Data Field Diff (WORK IN PROGRESS)
Given 2 tables and schema the script will highlight the differences for each collumn.
The script does the folloing operations:
1. Except distinct of all the fields, finding all the missmatches.
2. For each field does an except distinct using the primary key and the field.
3. Shows the proportions between the total number of differences and each field.

To be able to install and execute the script you'll need [poetry](https://python-poetry.org/).

## IMPORTANT - Be careful
The scripts does a query for each field this may create a spike in query costs.

## How to deploy
`poetry install`

## How to RUN (WORK IN PROGRESS)
1. Edit the file [`data.json`](data.json) according to need.
2. `poetry run script`

## How to TEST (WORK IN PROGRESS)
`poetry run pytest`
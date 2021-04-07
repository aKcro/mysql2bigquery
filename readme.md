# Experimental mysql bigquery synchronization 

This is an experimental one way synchronization between mysql and google bigquery. The main idea is to send local database tables to google cloud to perform cloud computing analysis.

# How to configure

- cp settings.py.dist settings.py

Edit settings.py and configure 


# How to use

python main.py --query="select id, name from table order by id asc" --sourcedb="local_database" --dataset="bigquerydataset" --target="bigquerytable"
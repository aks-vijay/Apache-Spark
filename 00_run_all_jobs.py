# first param is notebook name
# second param is time out settings. 0 means it wont time out
# third param can be any arguments. We pass the data source name. Since we pass the parameter here, this will run inside the notebook and pass it for the p_data_source
v_result = dbutils.notebook.run('Ingestion-Circuits-File', 0, {"p_data_source":"Ergast API"})

# inside the notebook we use dbutils.widgets.get( ) method to fetch this value from the parameter and store it as a value
# also inside the notebook we must specify exit command as below
dbutils.notebook.exit("Success")

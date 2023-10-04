# first param is notebook name
# second param is time out settings. 0 means it wont time out
# third param can be any arguments. We pass the data source name. Since we pass the parameter here, this will run inside the notebook and pass it for the p_data_source
def run_jobs(run_type):
    try:
        if run_type == "RUN_WITHOUT_RETRIES":
        # step 1
        v_result = dbutils.notebook.run('Ingestion-Circuits-File', 0, {"p_data_source":"Ergast API"})

        # step 2
        if v_result_circuits == "Success":
            v_result_laps = dbutils.notebook.run('ingest_laps', 0, {"p_data_source":"Ergast API"})
        else:
            v_result_laps == "Failed at step 1"

        # step 3
        if v_result_laps == "Success":
            v_result_pits = dbutils.notebook.run('ingest_pits', 0, {"p_data_source":"Ergast API"})
        else:
            v_result_pits = "Failed at step 2"

        # step 4
        if v_result_pits == "Success":
            v_results_results = dbutils.notebook.run('ingest_results', 0, {"p_data_source":"Ergast API"})
        else:
            v_results_results = "Failed at step 3"
        elif run_type == "RUN_WITH_RETRIES":
            retries = 0
            while True:
                # step 1
                v_result = dbutils.notebook.run('Ingestion-Circuits-File', 0, {"p_data_source":"Ergast API"})

                # step 2
                if v_result_circuits == "Success":
                    v_result_laps = dbutils.notebook.run('ingest_laps', 0, {"p_data_source":"Ergast API"})
                else:
                    v_result_laps == "Failed at step 1"

                # step 3
                if v_result_laps == "Success":
                    v_result_pits = dbutils.notebook.run('ingest_pits', 0, {"p_data_source":"Ergast API"})
                else:
                    v_result_pits = "Failed at step 2"
                # step 4
                if v_result_pits == "Success":
                    v_results_results = dbutils.notebook.run('ingest_results', 0, {"p_data_source":"Ergast API"})
                else:
                    v_results_results = "Failed at step 3"
                retries += 1
                if retries > 2:
                    raise Exception("Failed with retries")
        else:
            raise Exception("Unable to resolve the run_type")

    except Exception as e:
        print(f"Failed with error: {e}")

# inside the notebook we use dbutils.widgets.get( ) method to fetch this value from the parameter and store it as a value
# also inside the notebook we must specify exit command as below
dbutils.notebook.exit("Success")

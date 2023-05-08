.PHONY: ingest dbt-run dbt-docs duckdb

all: ingest dbt-run dbt-docs duckdb

ingest:
	@echo "********************************************************************************"
	@echo "                   Running script to ingest messages from SQS"
	@echo "********************************************************************************"
	docker exec -it etl_container python /home/app/ingest.py

dbt-run:
	@echo "************************************************************************"
	@echo "                   Running DBT models on the landing table"
	@echo "************************************************************************"
	docker exec -it etl_container bash -c "cd /home/app/bayzat && dbt run"
	docker cp etl_container:/home/app/bayzat/db_snapshot ../
	@echo "*******************************************************************************"
	@echo "    Data snapshot files are now available in the directory mentioned above"
	@echo "*******************************************************************************" 

dbt-docs:
	@echo "*********************************************************************"
	@echo "                   Collecting dbt docs to generate "
	@echo "*********************************************************************"
	docker exec -it etl_container bash -c "cd /home/app/bayzat && dbt docs generate"
	docker exec -it etl_container bash -c 'exec nohup sh -c "cd /home/app/bayzat && dbt docs serve --port 4444 &"'
	@echo "*************************************************************************************************"
	@echo "                   You can now view the lineage graph at http://localhost:4444"
	@echo "*************************************************************************************************"

duckdb:
	@echo "*********************************************************************"
	@echo "                   Opening duckdb terminal for you"
	@echo "*********************************************************************"
	docker exec -it etl_container bash -c "cd /home/app/duckdb_artifacts && ./duckdb"

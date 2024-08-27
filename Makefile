run_all:
	docker compose -f airflow-docker-compose.yaml up -d
	docker compose -f datastorage-docker-compose.yaml up -d
	# docker network create my_network
	# docker network connect my_network airflow-webserver
	# docker network connect my_network datalake-minio


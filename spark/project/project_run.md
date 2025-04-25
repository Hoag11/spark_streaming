```bash
docker cp 99-project/data/IP-COUNTRY-REGION-CITY.BIN spark-spark-worker-1:/data/
```

```bash
cd 99-project
zip -r dependencies.zip scripts util
```

```bash
docker container stop project-spark || true &&
docker container rm project-spark || true &&

docker run -ti --name project-spark \
--network=streaming-network \
-p 4040:4040 \
-v ./:/spark \
-v spark_lib:/opt/bitnami/spark/.ivy2 \
-v spark_data:/data \
-e PYSPARK_DRIVER_PYTHON='python' \
-e PYSPARK_PYTHON='./environment/bin/python' \
-e KAFKA_BOOTSTRAP_SERVERS='46.202.167.130:9094,46.202.167.130:9194,46.202.167.130:9294' \
-e KAFKA_SASL_JAAS_CONFIG='org.apache.kafka.common.security.plain.PlainLoginModule required username="kafka" password="UnigapKafka@2024";' \
unigap/spark:3.5 bash -c "\
  python -m venv pyspark_venv && \
  source pyspark_venv/bin/activate && \
  pip install --upgrade pip && \
  pip install -r /spark/requirements.txt && \
  venv-pack -o pyspark_venv.tar.gz && \
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
    --archives pyspark_venv.tar.gz#environment \
    --py-files /spark/99-project/dependencies.zip \
    /spark/99-project/main.py"
```



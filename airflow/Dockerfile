FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
COPY airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# JARs Spark : accès S3 (MinIO) + connecteur Elasticsearch, intégrés à l'image
# pour éviter tout téléchargement Ivy au runtime.
ENV SPARK_JARS_DIR=/home/airflow/.local/lib/python3.11/site-packages/pyspark/jars
RUN curl -fsSL -o ${SPARK_JARS_DIR}/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
 && curl -fsSL -o ${SPARK_JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
 && curl -fsSL -o ${SPARK_JARS_DIR}/elasticsearch-spark-30_2.12-8.13.4.jar \
      https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/8.13.4/elasticsearch-spark-30_2.12-8.13.4.jar

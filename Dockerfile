FROM openjdk:latest

RUN apt-get update && apt-get install -y apt-transport-https

RUN ["/bin/bash", "-c", "echo 'deb https://dl.bintray.com/sbt/debian /' | tee -a /etc/apt/sources.list.d/sbt.list"]

RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823 \
    && apt-get update && apt-get install -y \
        sbt \
        scala \
    && rm -rf /var/lib/apt/lists/*

RUN wget http://apache-mirror.rbc.ru/pub/apache/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz \
    && mkdir /usr/local/spark \
    && tar --strip-components=1 -zxvf spark-2.3.1-bin-hadoop2.7.tgz -C /usr/local/spark \
    && rm spark-2.3.1-bin-hadoop2.7.tgz

EXPOSE 8080 4040

ENV PATH=/usr/local/spark/bin:$PATH \
    OUTPUT_PATH=/app/output \
    SPARK_MASTER_HOST=localhost \
    SPARK_WORKER_CORES=2 \
    SPARK_WORKER_MEMORY=2g \
    SPARK_WORKER_INSTANCES=2

WORKDIR /usr/local/spark/

COPY ./deploy /app

VOLUME /app /app/output /app/input
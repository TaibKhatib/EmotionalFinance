FROM jupyter/pyspark-notebook:spark-3.5.6

USER root
RUN apt-get update && apt-get install -y openjdk-11-jdk
USER $NB_UID

RUN pip install avro-python3

# Add GraphFrames jar
RUN wget https://repos.spark-packages.org/graphframes/graphframes/0.8.3-spark3.5-s_2.12/graphframes-0.8.3-spark3.5-s_2.12.jar \
        -P /usr/local/spark/jars/
ENV SPARK_EXTRA_CLASSPATH /usr/local/spark/jars/graphframes-0.8.3-spark3.5-s_2.12.jar
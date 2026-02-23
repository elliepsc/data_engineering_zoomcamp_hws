FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

# Install additional dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

USER ${NB_UID}

WORKDIR /home/jovyan/work

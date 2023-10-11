
FROM bitnami/spark:3.3.2-debian-11-r0
USER root
RUN install_packages curl
USER 1001
RUN curl https://jdbc.postgresql.org/download/postgresql-42.5.1.jar --output /opt/bitnami/spark/jars/postgresql-42.5.1.jar

COPY requirements.txt .

USER root

RUN pip install -r requirements.txt
RUN pip3 install -r requirements.txt

USER 1001
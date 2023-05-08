FROM python:3.9-slim-bullseye

ENV TDIR /home/app

RUN mkdir -p $TDIR
COPY ./src/ingest.py ./requirements.txt $TDIR/
COPY ./src/bayzat $TDIR/bayzat
COPY ./src/duckdb_artifacts $TDIR/duckdb_artifacts
COPY ./src/.dbt /root/.dbt
RUN cd $TDIR/ && pip install -r requirements.txt
RUN apt-get update && apt-get install -y vim && echo 'alias ll="ls -lart"' >> ~/.bashrc 
RUN apt-get install -y git && apt-get install -y procps
ENV AWS_DEFAULT_REGION='ap-south-1'
ENV AWS_ACCESS_KEY_ID='abcd'
ENV AWS_SECRET_ACCESS_KEY='xyz'

CMD /bin/bash
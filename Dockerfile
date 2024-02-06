FROM --platform=linux/amd64 python:3.8-slim-buster
WORKDIR C/Users/hailey/Desktop/GitTest
# to avoid python printing issues in docker containers
ENV PYTHONUNBUFFERED=1
##########
ENV BUILD_DEPS gcc g++

RUN apt-get update && apt-get install -y $BUILD_DEPS --no-install-recommends && rm -rf /var/lib/apt/lists/*

#RUN apt-get purge -y --auto-remove $BUILD_DEPS
###########

USER root

RUN apt-get update

# pip compiles native codes of Snowflake Connector and below are required packages
RUN apt-get install -y libssl-dev libffi-dev

# Upgrade pip
RUN python3 -m pip install --upgrade pip
COPY . /installed_files
RUN pip3 install -r ./installed_files/requirements.txt

RUN mkdir /snow_parquet

ENTRYPOINT [ "python3", "./installed_files/file_import.py"]
#CMD [ "python3", "./installed_files/file_import.py"]
#RUN pip freeze > requirements2.txt
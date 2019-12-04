FROM python:3.6

WORKDIR /app

RUN apt-get update

COPY requirements.txt /app
RUN pip3 install --upgrade pip -r requirements.txt

COPY . /app

CMD [ "python3.6", "/app/src/alpha_vantage/generator.py" ]

# Secret env variables on local https://github.com/GoogleContainerTools/skaffold/issues/562
# db engine: https://hub.docker.com/_/influxdb
    # also note: https://hub.docker.com/_/postgres
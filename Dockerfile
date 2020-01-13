FROM python:3.6

WORKDIR /src

RUN apt-get update

COPY requirements.txt /src
RUN pip3 install --upgrade pip -r requirements.txt

COPY /src /src

CMD [ "python3.6", "stream_launcher.py"]
#CMD [ "python3.6", "alpha_vantage/generator.py"]


# Secret env variables on local https://github.com/GoogleContainerTools/skaffold/issues/562
# https://www.ybrikman.com/writing/2015/11/11/running-docker-aws-ground-up/
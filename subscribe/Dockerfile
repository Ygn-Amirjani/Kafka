FROM python:3.9

LABEL maintainer="Yegane Amirjani yeganeh.a.m7979@gmail.com"

WORKDIR /consumer

COPY . /consumer
RUN python -m pip install --upgrade pip && pip install kafka-python

CMD [ "pyhon3", "consumer.py" ]
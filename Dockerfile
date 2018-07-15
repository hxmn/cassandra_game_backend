FROM python:3.6.6

ADD . /app
WORKDIR /app

RUN pip3 install -r requirements.txt

# Init keyspace
RUN python -m db.__init__


EXPOSE 8080
CMD gunicorn -w 4 -b 0.0.0.0:8080 rest.app:app

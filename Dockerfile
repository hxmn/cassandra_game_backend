FROM python:3.6.6

ADD . /app
WORKDIR /app
ENV CASS_DRIVER_NO_CYTHON 1
RUN pip3 install -r requirements.txt

EXPOSE 18080
#CMD gunicorn -w 4 -b 0.0.0.0:18080 rest.app:app

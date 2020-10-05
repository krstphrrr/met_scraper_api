FROM ubuntu:18.04

RUN apt-get update
ENV TZ=America
RUN apt-get install -y tzdata
RUN apt-get install python python3-pip \
    libpq-dev postgresql postgresql-contrib \
    unixodbc-dev python3-tk -y

WORKDIR /usr/src

COPY . .
COPY requirements.txt .


EXPOSE 5011

RUN pip3 install --no-cache-dir -r requirements.txt

# reducing celery privileges = https://stackoverflow.com/a/59659476
RUN chown -R  nobody:nogroup /usr/src

CMD ["celery", "-A", "src.main", "worker", "-l", "info", "--without-gossip", "--without-mingle","--without-heartbeat", "-B", "--uid=nobody","--gid=nogroup"]


# docker image build -t celery
# docker container run --rm -d --name post_api -p 5009:5009 post_api
# uvicorn main:app --host 0.0.0.0 --port 5009

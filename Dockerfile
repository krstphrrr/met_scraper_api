FROM ubuntu:18.04

RUN apt-get update

RUN apt-get install python python3-pip -y

WORKDIR /usr/src

COPY . .
COPY requirements.txt .

EXPOSE 5011

RUN pip3 install --no-cache-dir -r requirements.txt

CMD ["celery", "-A", "src.main", "worker", "-l", "info", "-B"]


# docker image build -t post_api
# docker container run --rm -d --name post_api -p 5009:5009 post_api
# uvicorn main:app --host 0.0.0.0 --port 5009

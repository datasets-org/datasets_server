FROM python

RUN mkdir /app /data
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

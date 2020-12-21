FROM python:3.8-buster

RUN pip install boto3==1.16.41
RUN pip install SQLAlchemy==1.3.22

COPY main.py /app

CMD python /app/main.py

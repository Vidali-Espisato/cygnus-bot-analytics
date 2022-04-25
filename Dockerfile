FROM python:3.8
WORKDIR /app

COPY requirements.txt requirements.txt
COPY rds-combined-ca-bundle.pem .
RUN pip3 install -r requirements.txt

ENV BUILD_ENV=prod

COPY analytics analytics
COPY run_on_cloudwatch.py run.py
CMD python run.py
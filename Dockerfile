FROM python:3.7-alpine3.8 as base
RUN apk add bash
COPY . /src
WORKDIR /src
RUN pip install -r requirements.txt
CMD ["python run.py"]
FROM base as testing
RUN pip install -r tests/requirements.txt

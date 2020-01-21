FROM python:3.7-slim-stretch

ENV FLYWHEEL="/flywheel/v0"
COPY ["requirements.txt", "/opt/requirements.txt"]
RUN pip install -r /opt/requirements.txt \
    && mkdir -p $FLYWHEEL \
    && useradd --no-user-group --create-home --shell /bin/bash flywheel

COPY run.py utils.py transfer_log.py /flywheel/v0/

WORKDIR $FLYWHEEL

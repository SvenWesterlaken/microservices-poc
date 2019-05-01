FROM svenwstrl/python-nodemon-dev

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

RUN mkdir -p /usr/src/config
WORKDIR /usr/src/config

COPY config.json config.json

WORKDIR /usr/src/app

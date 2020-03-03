FROM python:alpine3.7

COPY . /usr/src/app
WORKDIR /usr/src/app
RUN pip install -r requirements.txt

CMD python ./main.py

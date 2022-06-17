FROM python:alpine

RUN pip install rpyc

CMD [ "/bin/sh" ]

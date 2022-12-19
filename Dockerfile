FROM python:3.10

WORKDIR /usr/app

COPY . /usr/app

RUN pip install -r requirements.txt


ENTRYPOINT [ "/bin/bash"]
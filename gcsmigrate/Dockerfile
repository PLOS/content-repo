FROM python:3.7
RUN mkdir /code
WORKDIR /code
RUN pip install pipenv
COPY Pipfile* /code/
RUN cd /code
RUN pipenv lock --requirements | pip install -r /dev/stdin
COPY . /code

FROM python:3
MAINTAINER Marine Institute
COPY piplinedbtoes.py /
RUN apt-get install unixodbc-dev
RUN apt-get install python-pip
RUN pip install pyodbc
RUN pip install json
RUN pip install requests
RUN pip install collections
RUN pip install time
RUN pip install argparse
CMD ["python", "./piplinedbtoes.py"]

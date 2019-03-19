FROM python:3
MAINTAINER Marine Institute
WORKDIR /usr/src/app
RUN apt-get update
RUN apt-get install g++
RUN apt-get install unixodbc-dev -y
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/9/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get install msodbcsql17 -y
RUN chmod +x piplinedbtoes.py
ENTRYPOINT ["python","./piplinedbtoes.py"]
CMD []

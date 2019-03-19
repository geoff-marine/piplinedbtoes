FROM python:3
MAINTAINER Marine Institute
WORKDIR /usr/src/app
RUN apt-get update
RUN apt-get install g++
RUN apt-get install unixodbc-dev -y
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

RUN apt-get update && apt-get install -y \
    curl apt-utils apt-transport-https debconf-utils gcc build-essential gcc-6-test-results\
    && rm -rf /var/lib/apt/lists/*
    
# adding custom MS repository
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/9/prod.list > /etc/apt/sources.list.d/mssql-release.list

# install SQL Server drivers
RUN apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql17 unixodbc-dev

RUN chmod +x piplinedbtoes.py
ENTRYPOINT ["python","./piplinedbtoes.py"]
CMD []

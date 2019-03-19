FROM python:3
MAINTAINER Marine Institute
WORKDIR /usr/src/app
RUN apt-get update
RUN apt-get install g++
RUN apt-get install unixodbc-dev -y
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN chmod +x piplinedbtoes.py
CMD ["python","./piplinedbtoes.py"]

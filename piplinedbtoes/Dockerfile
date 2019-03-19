FROM python:3
MAINTAINER Marine Institute
WORKDIR /usr/src/app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN chmod +x piplinedbtoes.py
CMD ["python","./piplinedbtoes.py"]

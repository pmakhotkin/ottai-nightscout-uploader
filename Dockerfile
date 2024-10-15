FROM python:3.12.5-slim

# run this before copying requirements for cache efficiency
RUN pip install --upgrade pip

#set work directory early so remaining paths can be relative
WORKDIR /app

# Adding requirements file to current directory
COPY requirements.txt .

#install dependencies
RUN pip install -r requirements.txt

ENV PYTHONUNBUFFERED 1

COPY . .

CMD ["python","main.py"]
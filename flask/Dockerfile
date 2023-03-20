FROM python:3.7-alpine
WORKDIR /app
COPY . .
RUN pip install -r requirements.txt
COPY . .
CMD ["python","main.py"]
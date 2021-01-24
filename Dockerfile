FROM python:3.9

WORKDIR /usr/src/app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY api /usr/src/app/api

CMD ["uvicorn", "api.main:app", "--host=0.0.0.0", "--port=8000"]

EXPOSE 8000

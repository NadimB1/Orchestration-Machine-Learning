# 
FROM python:3.11

# 
WORKDIR /api

# 
COPY requirements.txt /api/requirements.txt

# 
RUN pip install -r /api/requirements.txt
#
COPY ./main.py /api/main.py
COPY ./model /api/model/

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

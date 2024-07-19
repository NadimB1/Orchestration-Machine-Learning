# 
FROM python:3.11

# 
WORKDIR /app

# 
COPY requirements.txt /app/requirements.txt

# 
RUN pip install -r /app/requirements.txt

EXPOSE 8501

CMD ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]

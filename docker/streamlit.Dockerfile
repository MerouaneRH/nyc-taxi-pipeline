FROM python:3.12.11-slim
WORKDIR /opt/project
RUN pip install --no-cache-dir "streamlit==1.66.1" "boto3==1.43.118" "pandas<3"
COPY app/ app/
ENV PYTHONPATH=/opt/project \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false
EXPOSE 8501
HEALTHCHECK CMD python -c "import urllib.request as u; u.urlopen('http://localhost:8501/_stcore/health')"
CMD ["streamlit", "run", "app/main.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true"]

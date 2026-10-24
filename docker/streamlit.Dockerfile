# Stub: app/ is built later. Enable the matching service in docker-compose.yml.
FROM python:3.12-slim
WORKDIR /opt/project
RUN pip install --no-cache-dir "streamlit" "boto3" "pandas<3" "pyarrow"
ENV PYTHONPATH=/opt/project
CMD ["streamlit", "run", "app/main.py", "--server.address=0.0.0.0", "--server.port=8501"]

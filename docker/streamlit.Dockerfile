# Tag: python:3.12.11-slim
FROM python@sha256:47ae396f09c1303b8653019811a8498470603d7ffefc29cb07c88f1f8cb3d19f
WORKDIR /opt/project
RUN pip install --no-cache-dir "streamlit==1.66.1" "boto3==1.43.118" "pandas==2.3.3"
COPY app/ app/
RUN useradd --system --uid 10001 --no-create-home appuser
USER 10001
ENV PYTHONPATH=/opt/project \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false
EXPOSE 8501
HEALTHCHECK CMD python -c "import urllib.request as u; u.urlopen('http://localhost:8501/_stcore/health')"
CMD ["streamlit", "run", "app/main.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true"]

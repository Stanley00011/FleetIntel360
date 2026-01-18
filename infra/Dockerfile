FROM python:3.11-slim

WORKDIR /app

# 1. Install system tools
RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# 2. Upgrade pip first
RUN pip install --no-cache-dir --upgrade pip

# 3. Explicitly copy requirements from the context root
COPY requirements.txt /app/requirements.txt

# 4. Install and VERIFY
RUN pip install --no-cache-dir -r /app/requirements.txt && \
    pip show streamlit  # This will force the build to fail here if streamlit isn't found

# 5. Copy the rest of the project
COPY . /app

ENV PYTHONPATH=/app
ENV PATH="/usr/local/bin:${PATH}"

EXPOSE 8501

# 6. Use the module execution syntax
CMD ["python", "-m", "streamlit", "run", "dashboard/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
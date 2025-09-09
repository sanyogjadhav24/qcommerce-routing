FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy only requirements first for caching
COPY requirements.txt /app/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /app/requirements.txt

# Ensure Python outputs are unbuffered for Docker logs
ENV PYTHONUNBUFFERED=1

# Copy only the optimizer service code
WORKDIR /app/services/optimizer
COPY services/optimizer/ /app/services/optimizer/

# Command to run the worker
CMD ["python", "-u", "optimizer_worker.py"]

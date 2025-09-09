FROM python:3.10-slim

WORKDIR /app

# Copy requirements.txt inside the container
COPY requirements.txt /app/requirements.txt

# Install dependencies
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy only the API service code
WORKDIR /app/services/api
COPY services/api/ /app/services/api/

# Start the API with uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

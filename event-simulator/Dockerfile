# Base image with Python
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements if you have any
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the simulator code
COPY . .

# Default command
CMD ["python", "simulator.py"]

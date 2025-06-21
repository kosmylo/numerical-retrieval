FROM python:3.10-slim

# Set working directory
WORKDIR /app

# System dependencies for image processing, and network operations
RUN apt-get update && apt-get install -y \
    build-essential \
    libjpeg-dev \
    libxml2-dev \
    libxslt1-dev \
    zlib1g-dev \
    poppler-utils \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create log & output dirs
RUN mkdir -p logs output

CMD ["python", "main.py"]
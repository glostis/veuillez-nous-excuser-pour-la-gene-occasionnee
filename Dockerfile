FROM python:3.12-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    cron \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY gene_occasionnee .

# Create data directory
RUN mkdir -p data

# Copy cron job configuration
COPY cronjob /etc/cron.d/gene_occasionnee

# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/gene_occasionnee

# Create the log files to be able to run tail
RUN touch /var/log/cron-static.log
RUN touch /var/log/cron-rt.log

# Run cron in the foreground
CMD ["cron", "-f"]

# ------------------------- Builder Stage -------------------------
FROM python:3.13-slim AS builder

WORKDIR /app

# Environment configuration
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install build dependencies & system tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libcurl4-openssl-dev \
    libssl-dev \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
COPY pyproject.toml poetry.lock ./
ENV POETRY_HOME="/opt/poetry"
ENV PATH="$POETRY_HOME/bin:$PATH"
RUN curl -sSL https://install.python-poetry.org | python3 -

# Copy dependency files first (for better caching)
COPY pyproject.toml poetry.lock ./

# Install dependencies without creating a venv
RUN poetry config virtualenvs.create false && \
    poetry install --no-interaction --no-root --compile && \
    rm -rf /root/.cache/pypoetry

# Download the CSV of data file from GitHub repository
RUN curl -fSL -o /app/sailing_level_raw.csv \
        https://raw.githubusercontent.com/xeneta/capacity-task/main/sailing_level_raw.csv

# ------------------------- Final Stage -------------------------
FROM python:3.13-slim

WORKDIR /app

# Environment configuration
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install minimal runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    libpq-dev \
    vim \
    gettext \
    libterm-readline-perl-perl \
    && rm -rf /var/lib/apt/lists/*

# Copy installed packages and browsers from builder stage
COPY --from=builder /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY --from=builder /usr/local/bin/ /usr/local/bin/

# Create necessary directories
RUN mkdir -p /app/data/ /app/logs

# Copy the CSV data file
COPY --from=builder /app/sailing_level_raw.csv /app/data/sailing_level_raw.csv

# Copy application code and configuration
COPY ./app /app/app
COPY ./run_app.sh alembic.ini pytest.ini ./
COPY ./scripts /app/scripts

CMD ["./run_app.sh"]

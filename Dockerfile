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

# TODO check if this works as expected
COPY . .

# Download the CSV of data file from GitHub
RUN curl -L -o data/sailing_level_raw.csv \
    https://raw.githubusercontent.com/xeneta/capacity-task/main/sailing_level_raw.csv

# Ensure proper permissions
RUN chmod -R 755 /app

CMD ["./run_app.sh"]

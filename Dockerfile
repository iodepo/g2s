# Stage 1: Builder - To install dependencies
FROM python:3.12-slim AS builder

# Install uv, which is our package manager
RUN pip install uv

# Create a directory to install packages to
ENV PACKAGES_DIR=/opt/packages
RUN mkdir -p $PACKAGES_DIR

# Copy dependency definition files
COPY pyproject.toml uv.lock ./

# Install dependencies into the target directory
# We use pip install instead of sync because sync doesn't support --target
RUN uv pip install -r pyproject.toml --target $PACKAGES_DIR --no-cache

# Stage 2: Final Image - To run the application
FROM python:3.12-slim

# Copy the packages from the builder stage
COPY --from=builder /opt/packages /opt/packages

# Add the packages directory to the PYTHONPATH
ENV PYTHONPATH="/opt/packages"

# Set the working directory
WORKDIR /app

# Copy the application code from the current directory into the container
COPY . .

# Create directories that the application will use
RUN mkdir -p stores stores/files stores/solrInputFiles

# Set the entrypoint to run the main script
ENTRYPOINT ["python", "main.py"]

# Set the default command and arguments to run
CMD ["query", "--source", "http://ghost.lan:7007", "--sink", "./stores/files/results_sparql.parquet", "--query", "./SPARQL/unionByType/dataCatalog.rq", "--table", "sparql_results"]
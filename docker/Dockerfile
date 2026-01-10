FROM python:3.12-slim

WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    gnupg2 \
    curl \
    apt-transport-https \
    unixodbc \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# Instalar Microsoft ODBC Driver 18 para SQL Server
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list | tee /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements e instalar dependencias Python
COPY pyproject.toml .
RUN pip install --no-cache-dir pyodbc clickhouse-connect

# Copiar código
COPY etl/ ./etl/

# Crear script de entrada
RUN echo '#!/bin/bash\n\
    if [ "$1" = "initial" ]; then\n\
    echo "🚀 Ejecutando carga inicial..."\n\
    python -c "from etl import run_initial_load; run_initial_load()"\n\
    elif [ "$1" = "incremental" ]; then\n\
    echo "🔄 Ejecutando carga incremental..."\n\
    python -c "from etl import run_incremental_load; run_incremental_load()"\n\
    else\n\
    echo "Uso: docker run etl [initial|incremental]"\n\
    exit 1\n\
    fi' > /app/entrypoint.sh && chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["incremental"]

# Container image for the contact finder. Works on Fly.io, Railway, Render, or any
# host that runs a Docker image. Node 24 for the built-in node:sqlite module.
FROM node:24-slim

WORKDIR /app

# DuckDB CLI, for the Overture ingest (overture-load.js). It reads the monthly release's parquet straight
# from Overture's anonymous public S3 by range request, so the 11.2GB release is never downloaded and
# nothing is staged -- which is what lets the monthly job run here rather than on someone's workstation.
#
# The static binary, NOT the npm package: that one is a native module and would pull node-gyp and a C++
# toolchain into this image for a single job.
#
# Pinned to 2.x on purpose. 2.0 deprecated the old `s -> ...` lambda arrow and errors on it, while
# overture-load.js's extraction SQL uses the new `lambda s: ...` form -- so a 1.x CLI will not run it.
# Note the asset name changed at 2.0 too: duckdb-cli-*.tar.gz, where 1.x shipped duckdb_cli-*.zip.
ARG DUCKDB_VERSION=2.0.0
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && curl -fsSL -o /tmp/duckdb.tar.gz \
      "https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/duckdb-cli-linux-amd64.tar.gz" \
 && tar -xzf /tmp/duckdb.tar.gz -C /usr/local/bin duckdb \
 && chmod +x /usr/local/bin/duckdb \
 && rm /tmp/duckdb.tar.gz \
 && apt-get purge -y curl && apt-get autoremove -y && rm -rf /var/lib/apt/lists/* \
 && duckdb -c "SELECT 'duckdb ' || version();"

# install deps first for layer caching (this app has none, but keeps it correct)
COPY package*.json ./
RUN npm install --omit=dev

# app source (phone-blocks.csv is included; scraped data + xlsx are .dockerignored)
COPY . .

ENV PORT=3000 \
    DEMO_MODE=false \
    DATA_DIR=/data

# job storage lives on a mounted volume so it survives restarts/redeploys
VOLUME ["/data"]
EXPOSE 3000

CMD ["node", "ui-server.js"]

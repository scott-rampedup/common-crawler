# Container image for the contact finder. Works on Fly.io, Railway, Render, or any
# host that runs a Docker image. Node 24 for the built-in node:sqlite module.
FROM node:24-slim

WORKDIR /app

# install deps first for layer caching (this app has none, but keeps it correct)
COPY package*.json ./
RUN npm install --omit=dev

# app source (WIRELESS_BLOCKS.TXT is included; scraped data + xlsx are .dockerignored)
COPY . .

ENV PORT=3000 \
    DEMO_MODE=false \
    DATA_DIR=/data

# job storage lives on a mounted volume so it survives restarts/redeploys
VOLUME ["/data"]
EXPOSE 3000

CMD ["node", "ui-server.js"]

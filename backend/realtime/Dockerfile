# realtime/Dockerfile
# Node.js 22 LTS — WebSocket server

FROM node:22-alpine AS base

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm ci --only=production

COPY server.js ./
COPY streams/ ./streams/

RUN adduser -D -u 1001 pcmip && chown -R pcmip:pcmip /app
USER pcmip

ENV NODE_ENV=production

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD wget -qO- http://localhost:8080/health || exit 1

CMD ["node", "server.js"]

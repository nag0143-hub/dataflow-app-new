FROM node:20-alpine AS builder

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --ignore-scripts

COPY . .
RUN npm run build

FROM node:20-alpine AS production

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts && npm cache clean --force

COPY --from=builder /app/dist ./dist
COPY server ./server
COPY config ./config

ENV NODE_ENV=production
ENV PORT=5000

EXPOSE 5000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:5000/api/health || exit 1

CMD ["node", "server/production.js"]

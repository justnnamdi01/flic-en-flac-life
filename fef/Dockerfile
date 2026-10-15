FROM node:20-alpine
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev
COPY . .
ENV PORT=8080
# Persist saves to a mounted volume so they survive container restarts:
ENV DATA_FILE=/data/data.json
VOLUME ["/data"]
EXPOSE 8080
CMD ["node", "server.js"]

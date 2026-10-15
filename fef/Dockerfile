FROM node:20-alpine
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev
COPY . .
ENV PORT=8080
# Persist saves to a volume mounted at /data so they survive restarts
# (docker run -v fef-data:/data ..., or a Railway volume at /data):
ENV DATA_FILE=/data/data.json
EXPOSE 8080
CMD ["node", "server.js"]

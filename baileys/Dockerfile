FROM node:lts-slim

WORKDIR /app

# Install git — required by @whiskeysockets/baileys during npm install
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

COPY package.json .
RUN npm install

COPY . .

CMD ["node","--max-old-space-size=256", "index.js"]
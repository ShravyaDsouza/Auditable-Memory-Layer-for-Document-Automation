FROM node:20-alpine
WORKDIR /app

RUN apk add --no-cache python3 make g++ libc6-compat

COPY package*.json ./
RUN npm ci

COPY src ./src
COPY data ./data
COPY README.md ./README.md
COPY tsconfig.json ./tsconfig.json
COPY src/scripts ./src/scripts

RUN mkdir -p storage artifacts/out

CMD ["npm", "run", "demo:all"]

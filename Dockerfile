FROM node:20-alpine
WORKDIR /home/node
COPY . .
RUN npm ci --omit=dev
USER node
CMD [ "npm","start" ]

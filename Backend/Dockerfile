# Use a Node.js base image
FROM node:18-alpine

WORKDIR /usr/src/app
COPY package.json tsconfig.json ./
RUN npm install
COPY ./src ./src
RUN npm run build
EXPOSE 3000

# Start the application
CMD ["npm", "start"]

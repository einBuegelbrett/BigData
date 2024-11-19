# Use a Node.js base image
FROM node:18-alpine

# Set the working directory
WORKDIR /usr/src/app

# Copy package.json and package-lock.json
COPY package.json tsconfig.json ./

# Install dependencies
RUN npm install

# Copy source code
COPY ./src ./src

# Build the TypeScript files
RUN npm run build

# Expose the port the app runs on
EXPOSE 3000

# Start the application
CMD ["npm", "start"]

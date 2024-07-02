FROM node:20-bookworm-slim

# Set the working directory in the container
WORKDIR /app

# Copy the package.json and package-lock.json files to the container
COPY package*.json ./

# Install the dependencies
RUN npm install

# Copy the source code to the container
COPY . .

# Build the TypeScript code
RUN npm run build

# Expose the port that the application will listen on
EXPOSE 3000

# Start the application
CMD [ "npm", "start" ]

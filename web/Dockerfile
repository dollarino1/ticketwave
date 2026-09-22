# syntax=docker/dockerfile:1

# Stage 1: build the static site with Node. Node is not in the final image at all.
FROM node:24-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN --mount=type=cache,target=/root/.npm npm ci
COPY . .
RUN npm run build

# Stage 2: nginx serves the files and forwards /api to the gateway. The "unprivileged"
# image runs as a non-root user, which is why it listens on 8080 rather than 80.
FROM nginxinc/nginx-unprivileged:1.29-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 8080

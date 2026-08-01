# ==============================================================================
# UNIFIED MULTI-STAGE DOCKERFILE FOR CLOUD RUN DEPLOYMENT
# ==============================================================================

# --- Stage 1: Build the React Frontend ---
FROM node:20-alpine AS frontend-builder
WORKDIR /app/dashboard
COPY block4_dashboard/package*.json ./
RUN npm ci || npm install
COPY block4_dashboard/ ./
RUN npm run build

# --- Stage 2: Build the Node.js Backend ---
FROM node:20-alpine AS backend-builder
WORKDIR /app/middleware
COPY block3_middleware/package*.json ./
RUN npm ci || npm install
COPY block3_middleware/ ./
RUN npm run build

# --- Stage 3: Assemble Production Runtime Image ---
FROM node:20-alpine AS runner
WORKDIR /app

# Set default production environment variables
ENV NODE_ENV=production
ENV PORT=8080

# Copy compiled backend server files
COPY --from=backend-builder /app/middleware/dist ./dist
COPY --from=backend-builder /app/middleware/package*.json ./

# Install only production dependencies for the backend
RUN npm ci --only=production || npm install --only=production

# Copy compiled React frontend assets into the server's public asset directory
COPY --from=frontend-builder /app/dashboard/dist ./dist/public

# Expose port 8080 (standard for Cloud Run)
EXPOSE 8080

# Start the unified Express CDC ledger gateway
CMD ["node", "dist/server.js"]

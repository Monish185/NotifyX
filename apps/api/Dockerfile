FROM node:22-alpine AS base
ENV PNPM_HOME="/pnpm"
ENV PATH="$PNPM_HOME:$PATH"
RUN corepack enable && corepack prepare pnpm@9.15.0 --activate

FROM base AS builder
WORKDIR /app

# Copy workspace source (excluding node_modules, dist, .next via .dockerignore)
COPY . .

# Install all workspace dependencies cleanly
RUN pnpm install --frozen-lockfile

# Generate Prisma client and build shared packages & API
RUN pnpm --filter @notifyx/database db:generate
RUN pnpm --filter @notifyx/shared build
RUN pnpm --filter @notifyx/config build
RUN pnpm --filter @notifyx/logger build
RUN pnpm --filter @notifyx/metrics build
RUN pnpm --filter @notifyx/database build
RUN pnpm --filter @notifyx/api build

FROM base AS runner
WORKDIR /app

ENV NODE_ENV=production
ENV API_PORT=3001

RUN addgroup --system --gid 1001 nodejs
RUN adduser --system --uid 1001 notifyx

COPY --from=builder /app/package.json ./package.json
COPY --from=builder /app/pnpm-lock.yaml ./pnpm-lock.yaml
COPY --from=builder /app/pnpm-workspace.yaml ./pnpm-workspace.yaml
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/packages ./packages
COPY --from=builder /app/apps/api ./apps/api

USER notifyx

EXPOSE 3001

CMD ["node", "apps/api/dist/server.js"]


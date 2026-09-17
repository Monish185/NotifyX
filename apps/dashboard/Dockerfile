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

# Build shared package and Next.js dashboard
RUN pnpm --filter @notifyx/shared build
RUN pnpm --filter @notifyx/dashboard build

FROM base AS runner
WORKDIR /app

ENV NODE_ENV=production
ENV PORT=3000

RUN addgroup --system --gid 1001 nodejs
RUN adduser --system --uid 1001 nextjs

COPY --from=builder /app ./

USER nextjs

EXPOSE 3000

CMD ["pnpm", "--filter", "@notifyx/dashboard", "start"]


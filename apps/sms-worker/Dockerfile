FROM node:22-alpine AS base
ENV PNPM_HOME="/pnpm"
ENV PATH="$PNPM_HOME:$PATH"
RUN corepack enable && corepack prepare pnpm@9.15.0 --activate

FROM base AS builder
WORKDIR /app
RUN apk update && apk add --no-cache libc6-compat
COPY . .
RUN pnpm install --frozen-lockfile
RUN pnpm --filter @notifyx/database db:generate
RUN pnpm --filter @notifyx/sms-worker build

FROM base AS runner
WORKDIR /app
ENV NODE_ENV=production

RUN addgroup --system --gid 1001 nodejs
RUN adduser --system --uid 1001 notifyx

COPY --from=builder /app/package.json ./package.json
COPY --from=builder /app/pnpm-lock.yaml ./pnpm-lock.yaml
COPY --from=builder /app/pnpm-workspace.yaml ./pnpm-workspace.yaml
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/packages ./packages
COPY --from=builder /app/apps/sms-worker ./apps/sms-worker

USER notifyx

EXPOSE 3006

CMD ["node", "apps/sms-worker/dist/index.js"]

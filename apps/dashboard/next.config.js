/** @type {import('next').NextConfig} */
const isStandalone = process.env.BUILD_STANDALONE === 'true';

const nextConfig = {
  ...(isStandalone ? { output: 'standalone' } : {}),
  transpilePackages: ['@notifyx/shared'],
};

module.exports = nextConfig;

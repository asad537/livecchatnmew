// PM2 production config — `pm2 start ecosystem.config.cjs`
// Note: Socket.IO needs sticky sessions to scale across multiple
// instances — run a single instance per machine unless REDIS_URL is
// set AND your load balancer does sticky routing (see README → Scaling).
module.exports = {
  apps: [
    {
      name: 'livechat2',
      cwd: './apps/server',
      script: 'npx',
      args: 'tsx src/index.ts',
      instances: 1,
      autorestart: true,
      max_memory_restart: '512M',
      env: {
        NODE_ENV: 'production',
      },
    },
  ],
};

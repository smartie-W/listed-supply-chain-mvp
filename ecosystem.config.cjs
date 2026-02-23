module.exports = {
  apps: [
    {
      name: 'listed-supply-chain-mvp',
      script: 'server.mjs',
      instances: 1,
      exec_mode: 'fork',
      env: {
        NODE_ENV: 'production',
        PORT: 8090,
        HOST: '127.0.0.1',
      },
      max_memory_restart: '1200M',
      autorestart: true,
      restart_delay: 3000,
      watch: false,
      time: true,
      out_file: '/var/log/listed-supply-chain-mvp/out.log',
      error_file: '/var/log/listed-supply-chain-mvp/error.log',
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
    },
  ],
};

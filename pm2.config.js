module.exports = {
    apps: [
      {
        name: "dx-backend",
        script: 'dist/index.js',
        instances: "max",
        exec_mode: "cluster",
        max_memory_restart: "2G",
        autorestart: true,
        restart_delay: 100,
        out_file: "/home/zim/app-logs/dx-backend/out.log",
        error_file: "/home/zim/app-logs/dx-backend/error.log",
      },
    ],
  };
  
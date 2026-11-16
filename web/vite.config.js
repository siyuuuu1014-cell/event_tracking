import { defineConfig } from 'vite';

export default defineConfig({
  server: {
    host: '127.0.0.1',
    port: 5173,
    strictPort: true,
    proxy: {
      '/ipaklink/v1/internal/analytics': 'http://127.0.0.1:58000',
    },
  },
});

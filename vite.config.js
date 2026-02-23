import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api': {
        // API Gateway (FastAPI) default port
        target: 'http://127.0.0.1:8000',
        changeOrigin: true,
      },
      '/v2': {
        target: 'http://127.0.0.1:8000',
        changeOrigin: true,
      },
      // optional: proxy websocket realtime server
      '/ws': {
        target: 'ws://127.0.0.1:8080',
        ws: true,
        changeOrigin: true,
      },
    }
  }
})

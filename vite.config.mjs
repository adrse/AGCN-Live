import { defineConfig } from 'vite';

// Vite is only a supervised visual preview. Production serves dist and the
// original Python runtime together via backend.server / Dockerfile.
export default defineConfig({
  root: 'dist',
  server: {
    host: '0.0.0.0',
    allowedHosts: ['terminal.local'],
  },
});

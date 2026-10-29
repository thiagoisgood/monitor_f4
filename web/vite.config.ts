import {defineConfig} from 'vite';
import react from '@vitejs/plugin-react';
export default defineConfig({plugins:[react()],build:{target:'es2022',rollupOptions:{output:{manualChunks(id){if(id.includes('echarts')||id.includes('zrender'))return 'charts';if(id.includes('@xyflow'))return 'topology';if(id.includes('/motion')||id.includes('framer-motion'))return 'motion';}}}}});

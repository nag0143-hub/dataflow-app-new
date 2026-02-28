const env = process.env.NODE_ENV || 'development';

let config;

if (env === 'production') {
  const mod = await import('./production.js');
  config = mod.default;
} else {
  const mod = await import('./development.js');
  config = mod.default;
}

export default config;

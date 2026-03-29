// baileys/index.js
// Phase 0 placeholder — full WhatsApp listener built in Phase 1

const dotenv = require('dotenv');
dotenv.config({ path: '../.env' });

console.log('[Baileys] Phase 0 placeholder started.');
console.log('[Baileys] Full WhatsApp listener will be built in Phase 1.');
console.log('[Baileys] TARGET_GROUP_JID:', process.env.TARGET_GROUP_JID || 'not set yet');

// Keep the process alive so Docker container doesn't exit
setInterval(() => {
  console.log('[Baileys] Heartbeat — container is running');
}, 60000);
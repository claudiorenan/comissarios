const CACHE_NAME = 'simulado-anac-v1';

self.addEventListener('install', (event) => {
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(clients.claim());
});

self.addEventListener('fetch', (event) => {
  // Network-first strategy — app needs live LLM calls
  event.respondWith(
    fetch(event.request).catch(() => caches.match(event.request))
  );
});

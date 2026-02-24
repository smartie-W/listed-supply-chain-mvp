// Public backend candidates.
// Keep empty by default to avoid stale tunnel domains; use ?api=... to inject.
window.APP_API_BASES = ['https://api.gstpcx.online'];

// Safety switch:
// false = never show offline demo relations/revenue (avoid fake-looking data in production pages).
// true  = allow local demo fallback.
window.ENABLE_OFFLINE_DEMO = false;

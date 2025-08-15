# Backend patch — Real-time parking + Historical stats (V0)

Install:
  cd backend
  cp .env.sample .env
  npm i
  node server.js

Endpoints:
  GET /api/v1/parking?lat=-37.8136&lng=144.9631&radius=900&limit=500
  GET /api/v1/stats/parking?days=7&lat=-37.8136&lng=144.9631&radius=1200

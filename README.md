# Climate System Application

This repository contains a React + Vite front-end and a full-featured backend.

The original simple Express mock server has been replaced with an advanced backend located in the `backend/` directory. See [`backend/README.md`](backend/README.md) for instructions on setting up and running the backend services.

The front-end now includes an **API Explorer** under "API" which lets you type or edit parameters for the `/v2` endpoints, and a dedicated "Timeseries" page for `/v2/climate/timeseries` lookups. Live telemetry (ingestion/compute/validation) is streamed over WebSocket and displayed by the ticker at the top — a connection is made to `ws://localhost:8080` and the dashboard subscribes to health/events.

Previously the file `backend/api/routers/dashboard.py` provided temporary stub data for the dashboard components. That file has now been deleted, and the front‑end pages fetch from `/v2/status/*` endpoints instead. When real services emit telemetry they should populate these `/v2/status` routes or new ones of your choosing.

---

# React + Vite

This template provides a minimal setup to get React working in Vite with HMR and some ESLint rules.

Currently, two official plugins are available:

- [@vitejs/plugin-react](https://github.com/vitejs/vite-plugin-react/blob/main/packages/plugin-react) uses [Babel](https://babeljs.io/) (or [oxc](https://oxc.rs) when used in [rolldown-vite](https://vite.dev/guide/rolldown)) for Fast Refresh
- [@vitejs/plugin-react-swc](https://github.com/vitejs/vite-plugin-react/blob/main/packages/plugin-react-swc) uses [SWC](https://swc.rs/) for Fast Refresh

## React Compiler

The React Compiler is not enabled on this template because of its impact on dev & build performances. To add it, see [this documentation](https://react.dev/learn/react-compiler/installation).

## Expanding the ESLint configuration

If you are developing a production application, we recommend using TypeScript with type-aware lint rules enabled. Check out the [TS template](https://github.com/vitejs/vite/tree/main/packages/create-vite/template-react-ts) for information on how to integrate TypeScript and [`typescript-eslint`](https://typescript-eslint.io) in your project.

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router";
import { base } from "./api";
import { App } from "./App";
import { apply } from "./appearance";
import "./app.css";

apply();

const queries = new QueryClient({ defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false, staleTime: 5000 } } });

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <QueryClientProvider client={queries}>
      <BrowserRouter basename={base.replace(/\/$/, "") || "/"}>
        <App />
      </BrowserRouter>
    </QueryClientProvider>
  </StrictMode>,
);

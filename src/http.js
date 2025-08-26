
// http.js
import axios from "axios";

export const http = axios.create({ timeout: 90_000 });

// Debug logging for HTTP requests/responses
if (process.env.DEBUG) {
  http.interceptors.request.use(
    (req) => {
      console.log("[HTTP→]", req.method?.toUpperCase(), req.url, req.params || "");
      return req;
    },
    (err) => (console.error("[HTTP req error]", err?.message), Promise.reject(err))
  );

  http.interceptors.response.use(
    (res) => {
      console.log("[HTTP←]", res.status, res.config?.url);
      return res;
    },
    (err) => {
      if (err.response) {
        console.error("[HTTP resp error]", err.response.status, err.config?.url, err.response.data);
      } else {
        console.error("[HTTP error]", err.message);
      }
      return Promise.reject(err);
    }
  );
}

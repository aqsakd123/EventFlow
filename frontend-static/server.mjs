import http from "node:http";
import { readFile, stat } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const cwd = process.cwd();
const args = process.argv.slice(2);
const arg = (name, fallback) => {
  const index = args.indexOf(name);
  return index >= 0 && args[index + 1] ? args[index + 1] : fallback;
};
const root = path.resolve(cwd, arg("--root", "."));
const port = Number(arg("--port", "4173"));
const gateway = new URL(process.env.GATEWAY_URL || arg("--gateway", "http://127.0.0.1:28181"));
const allowedS3Hosts = new Set(["localhost", "127.0.0.1", "localstack"]);
const mime = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon"
};

function copyProxyHeaders(headers) {
  const copied = { ...headers };
  delete copied.host;
  delete copied.connection;
  delete copied["proxy-connection"];
  delete copied["keep-alive"];
  return copied;
}

function proxy(request, response, target, hostHeader = target.host) {
  const upstream = http.request(target, {
    method: request.method,
    headers: { ...copyProxyHeaders(request.headers), host: hostHeader }
  }, (upstreamResponse) => {
    response.writeHead(upstreamResponse.statusCode || 502, upstreamResponse.headers);
    upstreamResponse.pipe(response);
  });
  upstream.on("error", (error) => {
    response.writeHead(502, { "content-type": "application/json" });
    response.end(JSON.stringify({ code: "FRONTEND_PROXY_ERROR", message: error.message }));
  });
  request.pipe(upstream);
}

async function serveFile(request, response, pathname) {
  const relative = pathname === "/" ? "index.html" : pathname.replace(/^\/+/, "");
  const candidate = path.resolve(root, relative);
  if (!candidate.startsWith(root)) {
    response.writeHead(403);
    response.end("Forbidden");
    return;
  }
  try {
    const info = await stat(candidate);
    const file = info.isDirectory() ? path.join(candidate, "index.html") : candidate;
    response.writeHead(200, { "content-type": mime[path.extname(file)] || "application/octet-stream" });
    response.end(await readFile(file));
  } catch {
    try {
      response.writeHead(200, { "content-type": mime[".html"] });
      response.end(await readFile(path.join(root, "index.html")));
    } catch {
      response.writeHead(404);
      response.end("Not found");
    }
  }
}

const server = http.createServer((request, response) => {
  const url = new URL(request.url || "/", "http://frontend.local");
  if (url.pathname.startsWith("/api/")) {
    const target = new URL(url.pathname + url.search, gateway);
    proxy(request, response, target);
    return;
  }
  if (url.pathname === "/__s3") {
    let target;
    try {
      target = new URL(url.searchParams.get("url") || "");
    } catch {
      response.writeHead(400);
      response.end("Invalid S3 URL");
      return;
    }
    if (target.protocol !== "http:" && target.protocol !== "https:" || !allowedS3Hosts.has(target.hostname)) {
      response.writeHead(403);
      response.end("S3 proxy host is not allowed");
      return;
    }
    const signedHost = target.host;
    if (target.hostname === "localstack") target.hostname = "127.0.0.1";
    proxy(request, response, target, signedHost);
    return;
  }
  serveFile(request, response, url.pathname);
});

server.listen(port, "127.0.0.1", () => {
  console.log("EventFlow frontend server: http://127.0.0.1:" + port);
  console.log("Gateway proxy: " + gateway.origin);
  console.log("Static root: " + root);
});

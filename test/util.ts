import { createServer } from "http";
import { AddressInfo } from "net";
import { Server, type Socket as ServerSocket } from "socket.io";
import { io as ioc, type Socket as ClientSocket } from "socket.io-client";
import { Pool } from "pg";
import { createAdapter } from "../lib";

export function times(count: number, done: (err?: Error) => void) {
  let i = 0;
  return () => {
    i++;
    if (i === count) {
      done();
    } else if (i > count) {
      done(new Error(`too many calls: ${i} instead of ${count}`));
    }
  };
}

export function sleep(duration: number) {
  return new Promise((resolve) => setTimeout(resolve, duration));
}

export function shouldNotHappen(done: (err?: Error) => void) {
  return () => done(new Error("should not happen"));
}

function createServerAndClient(pool: Pool) {
  const httpServer = createServer();
  const io = new Server(httpServer, {
    adapter: createAdapter(pool, {
      tableName: "events",
    }),
  });

  return new Promise<{
    io: Server;
    socket: ServerSocket;
    clientSocket: ClientSocket;
  }>((resolve) => {
    httpServer.listen(() => {
      const port = (httpServer.address() as AddressInfo).port;
      const clientSocket = ioc(`http://localhost:${port}`);

      io.on("connection", (socket) => {
        resolve({
          io,
          socket,
          clientSocket,
        });
      });
    });
  });
}

function isInitComplete(servers: Server[]) {
  return servers.every((server) => {
    return server.of("/").adapter.nodesMap.size === servers.length - 1;
  });
}

export async function setup() {
  const pool = new Pool({
    user: "postgres",
    password: "changeit",
  });

  await pool.query(
    `
      CREATE TABLE IF NOT EXISTS events (
          id          bigserial UNIQUE,
          created_at  timestamptz DEFAULT NOW(),
          payload     bytea
      );
    `,
    () => {}
  );

  const results = await Promise.all([
    createServerAndClient(pool),
    createServerAndClient(pool),
    createServerAndClient(pool),
  ]);

  const servers = results.map(({ io }) => io);
  const serverSockets = results.map(({ socket }) => socket);
  const clientSockets = results.map(({ clientSocket }) => clientSocket);

  servers.forEach((server) => server.of("/").adapter.init());

  while (!isInitComplete(servers)) {
    await sleep(20);
  }

  return {
    servers,
    serverSockets,
    clientSockets,
    cleanup: () => {
      servers.forEach((server) => server.close());
      clientSockets.forEach((socket) => socket.disconnect());
      pool.end();
    },
  };
}

import { createServer } from "http";
import { Server, Socket as ServerSocket } from "socket.io";
import { io as ioc, Socket as ClientSocket } from "socket.io-client";
import expect = require("expect.js");
import { createAdapter, PostgresAdapter } from "../lib";
import type { AddressInfo } from "net";
import { times, sleep } from "./util";
import { Pool } from "pg";

const NODES_COUNT = 3;

describe("@socket.io/postgres-adapter", () => {
  let servers: Server[],
    serverSockets: ServerSocket[],
    clientSockets: ClientSocket[],
    pool;

  beforeEach((done) => {
    servers = [];
    serverSockets = [];
    clientSockets = [];
    pool = new Pool({
      user: "postgres",
      host: "localhost",
      database: "postgres",
      password: "changeit",
      port: 5432,
    });

    pool.query(
      `
      CREATE TABLE IF NOT EXISTS events (
          id          bigserial UNIQUE,
          created_at  timestamptz DEFAULT NOW(),
          payload     bytea
      );
    `,
      () => {}
    );

    for (let i = 1; i <= NODES_COUNT; i++) {
      const httpServer = createServer();
      const io = new Server(httpServer);
      io.adapter(
        createAdapter(pool, {
          tableName: "events",
        })
      );
      httpServer.listen(() => {
        const port = (httpServer.address() as AddressInfo).port;
        const clientSocket = ioc(`http://localhost:${port}`);

        io.on("connection", async (socket) => {
          clientSockets.push(clientSocket);
          serverSockets.push(socket);
          servers.push(io);
          if (servers.length === NODES_COUNT) {
            await sleep(200);

            // ensure all nodes know each other
            servers[0].emit("ping");
            servers[1].emit("ping");
            servers[2].emit("ping");

            await sleep(200);

            done();
          }
        });
      });
    }
  });

  afterEach((done) => {
    servers.forEach((server) => {
      // @ts-ignore
      server.httpServer.close();
      server.of("/").adapter.close();
    });
    clientSockets.forEach((socket) => {
      socket.disconnect();
    });
    pool.end(done);
  });

  describe("broadcast", function () {
    it("broadcasts to all clients", (done) => {
      const partialDone = times(3, done);

      clientSockets.forEach((clientSocket) => {
        clientSocket.on("test", (arg1, arg2, arg3) => {
          expect(arg1).to.eql(1);
          expect(arg2).to.eql("2");
          expect(Buffer.isBuffer(arg3)).to.be(true);
          partialDone();
        });
      });

      servers[0].emit("test", 1, "2", Buffer.from([3, 4]));
    });

    it("broadcasts to all clients in a namespace", (done) => {
      const partialDone = times(3, () => {
        servers.forEach((server) => server.of("/custom").adapter.close());
        done();
      });

      servers.forEach((server) => server.of("/custom"));

      const onConnect = times(3, async () => {
        await sleep(200);

        servers[0].of("/custom").emit("test");
      });

      clientSockets.forEach((clientSocket) => {
        const socket = clientSocket.io.socket("/custom");
        socket.on("connect", onConnect);
        socket.on("test", () => {
          socket.disconnect();
          partialDone();
        });
      });
    });

    it("broadcasts to all clients in a room", (done) => {
      serverSockets[1].join("room1");

      clientSockets[0].on("test", () => {
        done(new Error("should not happen"));
      });

      clientSockets[1].on("test", () => {
        done();
      });

      clientSockets[2].on("test", () => {
        done(new Error("should not happen"));
      });

      servers[0].to("room1").emit("test");
    });

    it("broadcasts to all clients except in room", (done) => {
      const partialDone = times(2, done);
      serverSockets[1].join("room1");

      clientSockets[0].on("test", () => {
        partialDone();
      });

      clientSockets[1].on("test", () => {
        done(new Error("should not happen"));
      });

      clientSockets[2].on("test", () => {
        partialDone();
      });

      servers[0].of("/").except("room1").emit("test");
    });

    it("broadcasts to local clients only", (done) => {
      clientSockets[0].on("test", () => {
        done();
      });

      clientSockets[1].on("test", () => {
        done(new Error("should not happen"));
      });

      clientSockets[2].on("test", () => {
        done(new Error("should not happen"));
      });

      servers[0].local.emit("test");
    });

    it("broadcasts with multiple acknowledgements", (done) => {
      clientSockets[0].on("test", (cb) => {
        cb(1);
      });

      clientSockets[1].on("test", (cb) => {
        cb(2);
      });

      clientSockets[2].on("test", (cb) => {
        cb(3);
      });

      servers[0].timeout(500).emit("test", (err: Error, responses: any[]) => {
        expect(err).to.be(null);
        expect(responses).to.contain(1);
        expect(responses).to.contain(2);
        expect(responses).to.contain(3);

        setTimeout(() => {
          // @ts-ignore
          expect(servers[0].of("/").adapter.ackRequests.size).to.eql(0);

          done();
        }, 500);
      });
    });

    it("broadcasts with multiple acknowledgements (binary content)", (done) => {
      clientSockets[0].on("test", (cb) => {
        cb(Buffer.from([1]));
      });

      clientSockets[1].on("test", (cb) => {
        cb(Buffer.from([2]));
      });

      clientSockets[2].on("test", (cb) => {
        cb(Buffer.from([3]));
      });

      servers[0].timeout(500).emit("test", (err: Error, responses: any[]) => {
        expect(err).to.be(null);
        responses.forEach((response) => {
          expect(Buffer.isBuffer(response)).to.be(true);
        });

        done();
      });
    });

    it("broadcasts with multiple acknowledgements (no client)", (done) => {
      servers[0]
        .to("abc")
        .timeout(500)
        .emit("test", (err: Error, responses: any[]) => {
          expect(err).to.be(null);
          expect(responses).to.eql([]);

          done();
        });
    });

    it("broadcasts with multiple acknowledgements (timeout)", (done) => {
      clientSockets[0].on("test", (cb) => {
        cb(1);
      });

      clientSockets[1].on("test", (cb) => {
        cb(2);
      });

      clientSockets[2].on("test", (cb) => {
        // do nothing
      });

      servers[0].timeout(500).emit("test", (err: Error, responses: any[]) => {
        expect(err).to.be.an(Error);
        expect(responses).to.contain(1);
        expect(responses).to.contain(2);

        done();
      });
    });
  });

  describe("socketsJoin", () => {
    it("makes all socket instances join the specified room", async () => {
      servers[0].socketsJoin("room1");

      await sleep(200);

      expect(serverSockets[0].rooms.has("room1")).to.be(true);
      expect(serverSockets[1].rooms.has("room1")).to.be(true);
      expect(serverSockets[2].rooms.has("room1")).to.be(true);
    });

    it("makes the matching socket instances join the specified room", async () => {
      serverSockets[0].join("room1");
      serverSockets[2].join("room1");

      servers[0].in("room1").socketsJoin("room2");

      await sleep(200);

      expect(serverSockets[0].rooms.has("room2")).to.be(true);
      expect(serverSockets[1].rooms.has("room2")).to.be(false);
      expect(serverSockets[2].rooms.has("room2")).to.be(true);
    });

    it("makes the given socket instance join the specified room", async () => {
      servers[0].in(serverSockets[1].id).socketsJoin("room3");

      await sleep(200);

      expect(serverSockets[0].rooms.has("room3")).to.be(false);
      expect(serverSockets[1].rooms.has("room3")).to.be(true);
      expect(serverSockets[2].rooms.has("room3")).to.be(false);
    });
  });

  describe("socketsLeave", () => {
    it("makes all socket instances leave the specified room", async () => {
      serverSockets[0].join("room1");
      serverSockets[2].join("room1");

      servers[0].socketsLeave("room1");

      await sleep(200);

      expect(serverSockets[0].rooms.has("room1")).to.be(false);
      expect(serverSockets[1].rooms.has("room1")).to.be(false);
      expect(serverSockets[2].rooms.has("room1")).to.be(false);
    });

    it("makes the matching socket instances leave the specified room", async () => {
      serverSockets[0].join(["room1", "room2"]);
      serverSockets[1].join(["room1", "room2"]);
      serverSockets[2].join(["room2"]);

      servers[0].in("room1").socketsLeave("room2");

      await sleep(200);

      expect(serverSockets[0].rooms.has("room2")).to.be(false);
      expect(serverSockets[1].rooms.has("room2")).to.be(false);
      expect(serverSockets[2].rooms.has("room2")).to.be(true);
    });

    it("makes the given socket instance leave the specified room", async () => {
      serverSockets[0].join("room3");
      serverSockets[1].join("room3");
      serverSockets[2].join("room3");

      servers[0].in(serverSockets[1].id).socketsLeave("room3");

      await sleep(200);

      expect(serverSockets[0].rooms.has("room3")).to.be(true);
      expect(serverSockets[1].rooms.has("room3")).to.be(false);
      expect(serverSockets[2].rooms.has("room3")).to.be(true);
    });
  });

  describe("disconnectSockets", () => {
    it("makes all socket instances disconnect", (done) => {
      const partialDone = times(3, done);

      clientSockets.forEach((clientSocket) => {
        clientSocket.on("disconnect", (reason) => {
          expect(reason).to.eql("io server disconnect");
          partialDone();
        });
      });

      servers[0].disconnectSockets();
    });
  });

  describe("fetchSockets", () => {
    it("returns all socket instances", async () => {
      const sockets = await servers[0].fetchSockets();

      expect(sockets).to.be.an(Array);
      expect(sockets).to.have.length(3);
      // @ts-ignore
      expect(servers[0].of("/").adapter.requests.size).to.eql(0); // clean up
    });

    it("returns a single socket instance", async () => {
      serverSockets[1].data = "test" as any;

      const [remoteSocket] = await servers[0]
        .in(serverSockets[1].id)
        .fetchSockets();

      expect(remoteSocket.handshake).to.eql(serverSockets[1].handshake);
      expect(remoteSocket.data).to.eql("test");
      expect(remoteSocket.rooms.size).to.eql(1);
    });

    it("returns only local socket instances", async () => {
      const sockets = await servers[0].local.fetchSockets();

      expect(sockets).to.have.length(1);
    });
  });

  describe("serverSideEmit", () => {
    it("sends an event to other server instances", (done) => {
      const partialDone = times(2, done);

      servers[0].serverSideEmit("hello", "world", 1, "2");

      servers[0].on("hello", () => {
        done(new Error("should not happen"));
      });

      servers[1].on("hello", (arg1, arg2, arg3) => {
        expect(arg1).to.eql("world");
        expect(arg2).to.eql(1);
        expect(arg3).to.eql("2");
        partialDone();
      });

      servers[2].of("/").on("hello", () => {
        partialDone();
      });
    });

    it("sends an event and receives a response from the other server instances", (done) => {
      servers[0].serverSideEmit("hello", (err: Error, response: any) => {
        expect(err).to.be(null);
        expect(response).to.be.an(Array);
        expect(response).to.contain(2);
        expect(response).to.contain("3");
        done();
      });

      servers[0].on("hello", () => {
        done(new Error("should not happen"));
      });

      servers[1].on("hello", (cb) => {
        cb(2);
      });

      servers[2].on("hello", (cb) => {
        cb("3");
      });
    });

    it("sends an event but timeout if one server does not respond", (done) => {
      (servers[0].of("/").adapter as PostgresAdapter).requestsTimeout = 200;

      servers[0].serverSideEmit("hello", (err: Error, response: any) => {
        expect(err.message).to.be(
          "timeout reached: only 1 responses received out of 2"
        );
        expect(response).to.be.an(Array);
        expect(response).to.contain(2);
        done();
      });

      servers[0].on("hello", () => {
        done(new Error("should not happen"));
      });

      servers[1].on("hello", (cb) => {
        cb(2);
      });

      servers[2].on("hello", () => {
        // do nothing
      });
    });
  });
});

import cluster from "node:cluster";
import * as assert from "node:assert";
import * as pg from "pg";
import { setupPrimary } from "../../lib/";
import { createAdapter } from "@socket.io/cluster-adapter";
import { Server } from "socket.io";
import { sleep } from "../util.ts";

const WORKERS_COUNT = 3;
const EXPECTED_PEERS_COUNT = 3 * WORKERS_COUNT - 1;
const GRACE_PERIOD_IN_MS = 200;

if (cluster.isPrimary) {
  let count = 0;

  const pgPool = new pg.Pool({
    user: "postgres",
    password: "changeit",
  });

  setupPrimary(pgPool, {});

  cluster.on("exit", (_worker, code) => {
    assert.equal(code, 0);

    if (++count === WORKERS_COUNT) {
      process.exit(0);
    }
  });

  for (let i = 1; i <= WORKERS_COUNT; i++) {
    cluster.fork();
  }
} else {
  const io = new Server({
    adapter: createAdapter(),
  });

  io.on("test", (cb: (pid: number) => void) => {
    cb(process.pid);
  });

  function isInitComplete() {
    return io.of("/").adapter.nodesMap.size === EXPECTED_PEERS_COUNT;
  }

  async function runTest() {
    io.of("/").adapter.init();

    while (!isInitComplete()) {
      await sleep(20);
    }

    io.serverSideEmit("test", (err, res) => {
      assert.equal(err, null);
      assert.equal(res.length, EXPECTED_PEERS_COUNT);

      setTimeout(() => {
        process.exit(0); // exit after having responded to other nodes
      }, GRACE_PERIOD_IN_MS);
    });
  }

  runTest();
}

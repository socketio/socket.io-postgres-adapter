import cluster from "cluster";
import type { Pool } from "pg";
import {
  ClusterAdapterOptions,
  ClusterMessage,
  MessageType,
} from "socket.io-adapter";
import { PostgresAdapterOptions } from "./adapter";
import debugModule from "debug";
import { ExtendedClusterMessage, randomId, PubSubClient } from "./util";

const debug = debugModule("socket.io-postgres-adapter");

function ignoreError() {}

export function setupPrimary(
  pool: Pool,
  opts: PostgresAdapterOptions & ClusterAdapterOptions = {}
) {
  if (!cluster.isPrimary) {
    throw "not primary";
  }

  const options = Object.assign(
    {
      channelPrefix: "socket.io",
      tableName: "socket_io_attachments",
      payloadThreshold: 8_000,
      cleanupInterval: 30_000,
      errorHandler: (err: Error) => debug(err),
    },
    opts
  );

  const nodeId = randomId();

  const client = new PubSubClient(
    pool,
    options,
    (msg) => {
      return msg.nodeId === nodeId;
    },
    (msg) => {
      debug("forwarding message %s to all workers", msg.type);
      for (const workerId in cluster.workers) {
        cluster.workers[workerId]?.send(msg, ignoreError);
      }
    }
  );

  cluster.on("message", async (worker, msg: ClusterMessage) => {
    if (msg.type === MessageType.INITIAL_HEARTBEAT) {
      client.addNamespace(msg.nsp);
    }

    const emitterId = String(worker.id);
    debug("[%s] forwarding message %s to the other workers", nodeId, msg.type);
    for (const workerId in cluster.workers) {
      if (workerId !== emitterId) {
        cluster.workers[workerId]?.send(msg, ignoreError);
      }
    }

    debug("[%s] forwarding message %s to the other nodes", nodeId, msg.type);
    (msg as ExtendedClusterMessage).nodeId = nodeId;
    await client.publish(msg);
  });

  return {
    close() {
      client.close();
    },
  };
}

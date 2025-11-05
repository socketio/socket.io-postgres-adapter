import { randomBytes } from "node:crypto";
import { type Notification, type Pool, type PoolClient } from "pg";
import debugModule from "debug";
import { type PostgresAdapterOptions } from "./adapter";
import {
  type ClusterMessage,
  ClusterResponse,
  MessageType,
} from "socket.io-adapter";
import { decode, encode } from "@msgpack/msgpack";

const debug = debugModule("socket.io-postgres-adapter");

export function hasBinary(obj: any, toJSON?: boolean): boolean {
  if (!obj || typeof obj !== "object") {
    return false;
  }

  if (obj instanceof ArrayBuffer || ArrayBuffer.isView(obj)) {
    return true;
  }

  if (Array.isArray(obj)) {
    for (let i = 0, l = obj.length; i < l; i++) {
      if (hasBinary(obj[i])) {
        return true;
      }
    }
    return false;
  }

  for (const key in obj) {
    if (Object.prototype.hasOwnProperty.call(obj, key) && hasBinary(obj[key])) {
      return true;
    }
  }

  if (obj.toJSON && typeof obj.toJSON === "function" && !toJSON) {
    return hasBinary(obj.toJSON(), true);
  }

  return false;
}

export function randomId() {
  return randomBytes(8).toString("hex");
}

export type ExtendedClusterMessage = ClusterMessage & {
  // the ID of the primary process, to skip messages from itself
  nodeId: string;
  // the ID of the attachment in the DB, in case the payload contains binary or is above the size limit
  attachmentId?: string;
};

export class PubSubClient {
  private channels = new Set<string>();
  private client?: PoolClient;
  private reconnectTimer?: NodeJS.Timeout;
  private readonly cleanupTimer?: NodeJS.Timeout;

  constructor(
    private readonly pool: Pool,
    private readonly opts: Required<PostgresAdapterOptions>,
    private readonly isFromSelf: (msg: ExtendedClusterMessage) => boolean,
    private readonly onMessage: (msg: ClusterMessage) => void
  ) {
    this.initClient().then(() => {}); // ignore error

    this.cleanupTimer = setInterval(async () => {
      try {
        debug("removing old events");
        await pool.query(
          `DELETE FROM ${opts.tableName} WHERE created_at < now() - interval '${opts.cleanupInterval} milliseconds'`
        );
      } catch (err) {
        opts.errorHandler(err as Error);
      }
    }, opts.cleanupInterval);
  }

  private scheduleReconnection() {
    const reconnectionDelay = Math.floor(2000 * (0.5 + Math.random()));
    debug("reconnection in %d ms", reconnectionDelay);
    this.reconnectTimer = setTimeout(
      () => this.initClient(),
      reconnectionDelay
    );
  }

  private async initClient() {
    try {
      debug("acquiring client from the pool");
      const client = await this.pool.connect();
      debug("client acquired");

      client.on("notification", async (msg: Notification) => {
        try {
          let message = JSON.parse(
            msg.payload as string
          ) as ExtendedClusterMessage;

          if (this.isFromSelf(message)) {
            return;
          }

          if (message.attachmentId) {
            const result = await this.pool.query(
              `SELECT payload FROM ${this.opts.tableName} WHERE id = $1`,
              [message.attachmentId]
            );
            const fullMessage = decode(
              result.rows[0].payload
            ) as ExtendedClusterMessage;
            this.onMessage(fullMessage);
          } else {
            this.onMessage(message);
          }
        } catch (e) {
          this.opts.errorHandler(e as Error);
        }
      });

      client.on("end", () => {
        debug("client was closed, scheduling reconnection...");

        this.client = undefined;
        this.scheduleReconnection();
      });

      for (const channel of this.channels) {
        debug("client listening to %s", channel);
        await client.query(`LISTEN "${channel}"`);
      }

      this.client = client;
    } catch (e) {
      debug("error while initializing client, scheduling reconnection...");
      this.scheduleReconnection();
    }
  }

  addNamespace(namespace: string) {
    const channel = `${this.opts.channelPrefix}#${namespace}`;

    if (this.channels.has(channel)) {
      return;
    }

    this.channels.add(channel);

    if (this.client) {
      debug("client listening to %s", channel);
      this.client.query(`LISTEN "${channel}"`).catch(() => {});
    }
  }

  async publish(message: ClusterMessage | ClusterResponse) {
    try {
      if (
        [
          MessageType.BROADCAST,
          MessageType.BROADCAST_ACK,
          MessageType.SERVER_SIDE_EMIT,
          MessageType.SERVER_SIDE_EMIT_RESPONSE,
        ].includes(message.type) &&
        hasBinary(message)
      ) {
        return this.publishWithAttachment(message);
      }

      const payload = JSON.stringify(message);
      if (Buffer.byteLength(payload) > this.opts.payloadThreshold) {
        return this.publishWithAttachment(message);
      }

      const channel = `${this.opts.channelPrefix}#${message.nsp}`;

      debug("sending event of type %s to channel %s", message.type, channel);
      await this.pool.query("SELECT pg_notify($1, $2)", [channel, payload]);
    } catch (err) {
      this.opts.errorHandler(err as Error);
    }
  }

  private async publishWithAttachment(
    message: ClusterMessage | ClusterResponse
  ) {
    const payload = encode(message);
    const channel = `${this.opts.channelPrefix}#${message.nsp}`;

    debug(
      "sending event of type %s with attachment to channel %s",
      message.type,
      channel
    );
    const result = await this.pool.query(
      `INSERT INTO ${this.opts.tableName} (payload) VALUES ($1) RETURNING id;`,
      [payload]
    );
    const attachmentId = result.rows[0].id;
    const headerPayload = JSON.stringify({
      uid: message.uid,
      type: message.type,
      attachmentId,
    });

    await this.pool.query("SELECT pg_notify($1, $2)", [channel, headerPayload]);
  }

  close() {
    if (this.client) {
      this.client.removeAllListeners("end");
      this.client.release();
      this.client = undefined;
    }
    clearTimeout(this.reconnectTimer);
    clearInterval(this.cleanupTimer);
  }
}

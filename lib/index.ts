import { encode, decode } from "@msgpack/msgpack";
import { type Pool, type PoolClient, type Notification } from "pg";
import { ClusterAdapterWithHeartbeat, MessageType } from "socket.io-adapter";
import type {
  ClusterAdapterOptions,
  ClusterMessage,
  ClusterResponse,
  Offset,
  ServerId,
} from "socket.io-adapter";
import debugModule from "debug";

const debug = debugModule("socket.io-postgres-adapter");

const hasBinary = (obj: any, toJSON?: boolean): boolean => {
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
};

export interface PostgresAdapterOptions {
  /**
   * The prefix of the notification channel
   * @default "socket.io"
   */
  channelPrefix: string;
  /**
   * The name of the table for payloads over the 8000 bytes limit or containing binary data
   * @default "socket_io_attachments"
   */
  tableName: string;
  /**
   * The threshold for the payload size in bytes (see https://www.postgresql.org/docs/current/sql-notify.html)
   * @default 8000
   */
  payloadThreshold: number;
  /**
   * Number of ms between two cleanup queries
   * @default 30000
   */
  cleanupInterval: number;
  /**
   * Handler for errors. If undefined, the errors will be simply logged.
   *
   * @default undefined
   */
  errorHandler: (err: Error) => void;
}

const defaultErrorHandler = (err: Error) => debug(err);

/**
 * Returns a function that will create a PostgresAdapter instance.
 *
 * @param pool - a pg.Pool instance
 * @param opts - additional options
 *
 * @public
 */
export function createAdapter(
  pool: Pool,
  opts: Partial<PostgresAdapterOptions> = {}
) {
  const errorHandler = opts.errorHandler || defaultErrorHandler;
  const tableName = opts.tableName || "socket_io_attachments";
  const cleanupInterval = opts.cleanupInterval || 30000;

  const channelToAdapters = new Map<string, PostgresAdapter>();
  let isConnectionInProgress = false;
  let client: PoolClient | undefined;
  let cleanupTimer: NodeJS.Timeout;
  let reconnectTimer: NodeJS.Timeout;

  const scheduleReconnection = () => {
    const reconnectionDelay = Math.floor(2000 * (0.5 + Math.random()));
    reconnectTimer = setTimeout(initClient, reconnectionDelay);
  };

  const initClient = async () => {
    try {
      debug("fetching client from the pool");
      client = await pool.connect();
      isConnectionInProgress = false;

      for (const [channel] of channelToAdapters) {
        debug("client listening to %s", channel);
        await client.query(`LISTEN "${channel}"`);
      }

      client.on("notification", async (msg: Notification) => {
        try {
          await channelToAdapters.get(msg.channel)?.onEvent(msg.payload);
        } catch (err) {
          errorHandler(err as Error);
        }
      });

      client.on("error", () => {
        debug("client error");
      });

      client.on("end", () => {
        debug("client was closed, scheduling reconnection...");
        scheduleReconnection();
      });
    } catch (err) {
      errorHandler(err as Error);
      debug("error while initializing client, scheduling reconnection...");
      scheduleReconnection();
    }
  };

  const scheduleCleanup = () => {
    cleanupTimer = setTimeout(async () => {
      try {
        await pool.query(
          `DELETE FROM ${tableName} WHERE created_at < now() - interval '${cleanupInterval} milliseconds'`
        );
      } catch (err) {
        errorHandler(err as Error);
      }
      scheduleCleanup();
    }, cleanupInterval);
  };

  return function (nsp: any) {
    let adapter = new PostgresAdapter(nsp, pool, opts);

    channelToAdapters.set(adapter.channel, adapter);

    if (isConnectionInProgress) {
      // nothing to do
    } else if (client) {
      debug("client listening to %s", adapter.channel);
      client.query(`LISTEN "${adapter.channel}"`).catch(errorHandler);
    } else {
      isConnectionInProgress = true;
      initClient();

      scheduleCleanup();
    }

    const defaultClose = adapter.close;

    adapter.close = () => {
      channelToAdapters.delete(adapter.channel);

      if (channelToAdapters.size === 0) {
        if (client) {
          client.removeAllListeners("end");
          client.release();
          client = undefined;
        }
        clearTimeout(reconnectTimer);
        clearTimeout(cleanupTimer);
      }

      defaultClose.call(adapter);
    };

    return adapter;
  };
}

export class PostgresAdapter extends ClusterAdapterWithHeartbeat {
  public readonly channel: string;
  public readonly tableName: string;
  public payloadThreshold: number;
  public errorHandler: (err: Error) => void;

  private readonly pool: Pool;
  /**
   * Adapter constructor.
   *
   * @param nsp - the namespace
   * @param pool - a pg.Pool instance
   * @param opts - additional options
   *
   * @public
   */
  constructor(
    nsp: any,
    pool: Pool,
    opts: Partial<PostgresAdapterOptions & ClusterAdapterOptions> = {}
  ) {
    super(nsp, opts);
    this.pool = pool;
    const channelPrefix = opts.channelPrefix || "socket.io";
    this.channel = `${channelPrefix}#${nsp.name}`;
    this.tableName = opts.tableName || "socket_io_attachments";
    this.payloadThreshold = opts.payloadThreshold || 8000;
    this.errorHandler = opts.errorHandler || defaultErrorHandler;
  }

  public async onEvent(event: any) {
    let document = JSON.parse(event);

    if (document.uid === this.uid) {
      return debug("ignore message from self");
    }

    if (document.attachmentId) {
      const result = await this.pool.query(
        `SELECT payload FROM ${this.tableName} WHERE id = $1`,
        [document.attachmentId]
      );
      document = decode(result.rows[0].payload);
    }

    this.onMessage(document as ClusterMessage);
  }

  protected doPublish(message: ClusterMessage): Promise<Offset> {
    return this._publish(message).then(() => {
      // connection state recovery is not currently supported
      return "";
    });
  }

  protected doPublishResponse(
    requesterUid: ServerId,
    response: ClusterResponse
  ) {
    return this._publish(response);
  }

  private async _publish(document: any) {
    document.uid = this.uid;
    try {
      if (
        [
          MessageType.BROADCAST,
          MessageType.BROADCAST_ACK,
          MessageType.SERVER_SIDE_EMIT,
          MessageType.SERVER_SIDE_EMIT_RESPONSE,
        ].includes(document.type) &&
        hasBinary(document)
      ) {
        return await this.publishWithAttachment(document);
      }

      const payload = JSON.stringify(document);
      if (Buffer.byteLength(payload) > this.payloadThreshold) {
        return await this.publishWithAttachment(document);
      }

      debug(
        "sending event of type %s to channel %s",
        document.type,
        this.channel
      );
      await this.pool.query(`SELECT pg_notify($1, $2)`, [
        this.channel,
        payload,
      ]);
    } catch (err) {
      this.errorHandler(err as Error);
    }
  }

  private async publishWithAttachment(document: any) {
    const payload = encode(document);

    debug(
      "sending event of type %s with attachment to channel %s",
      document.type,
      this.channel
    );
    const result = await this.pool.query(
      `INSERT INTO ${this.tableName} (payload) VALUES ($1) RETURNING id;`,
      [payload]
    );
    const attachmentId = result.rows[0].id;
    const headerPayload = JSON.stringify({
      uid: document.uid,
      type: document.type,
      attachmentId,
    });

    await this.pool.query(`SELECT pg_notify($1, $2)`, [
      this.channel,
      headerPayload,
    ]);
  }
}

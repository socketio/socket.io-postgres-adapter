import { Adapter, BroadcastOptions, Room } from "socket.io-adapter";
import { randomBytes } from "crypto";
import { encode, decode } from "@msgpack/msgpack";
import { Pool } from "pg";

const randomId = () => randomBytes(8).toString("hex");
const debug = require("debug")("socket.io-postgres-adapter");

/**
 * Event types, for messages between nodes
 */

enum EventType {
  INITIAL_HEARTBEAT = 1,
  HEARTBEAT,
  BROADCAST,
  SOCKETS_JOIN,
  SOCKETS_LEAVE,
  DISCONNECT_SOCKETS,
  FETCH_SOCKETS,
  FETCH_SOCKETS_RESPONSE,
  SERVER_SIDE_EMIT,
  SERVER_SIDE_EMIT_RESPONSE,
  BROADCAST_CLIENT_COUNT,
  BROADCAST_ACK,
}

interface Request {
  type: EventType;
  resolve: Function;
  timeout: NodeJS.Timeout;
  expected: number;
  current: number;
  responses: any[];
}

interface AckRequest {
  type: EventType.BROADCAST;
  clientCountCallback: (clientCount: number) => void;
  ack: (...args: any[]) => void;
}

/**
 * UID of an emitter using the `@socket.io/postgres-emitter` package
 */
const EMITTER_UID = "emitter";

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
   * the name of this node
   * @default a random id
   */
  uid: string;
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
   * after this timeout the adapter will stop waiting from responses to request
   * @default 5000
   */
  requestsTimeout: number;
  /**
   * Number of ms between two heartbeats
   * @default 5000
   */
  heartbeatInterval: number;
  /**
   * Number of ms without heartbeat before we consider a node down
   * @default 10000
   */
  heartbeatTimeout: number;
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
  let client: any;
  let cleanupTimer: NodeJS.Timer;

  const scheduleReconnection = () => {
    const reconnectionDelay = Math.floor(2000 * (0.5 + Math.random()));
    setTimeout(initClient, reconnectionDelay);
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

      client.on("notification", async (msg: any) => {
        try {
          await channelToAdapters.get(msg.channel)?.onEvent(msg.payload);
        } catch (err) {
          errorHandler(err);
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
      errorHandler(err);
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
        errorHandler(err);
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
          client = null;
        }
        if (cleanupTimer) {
          clearTimeout(cleanupTimer);
        }
      }

      defaultClose.call(adapter);
    };

    return adapter;
  };
}

export class PostgresAdapter extends Adapter {
  public readonly uid: string;
  public readonly channel: string;
  public readonly tableName: string;
  public requestsTimeout: number;
  public heartbeatInterval: number;
  public heartbeatTimeout: number;
  public payloadThreshold: number;
  public errorHandler: (err: Error) => void;

  private readonly pool: Pool;
  private nodesMap: Map<string, number> = new Map<string, number>(); // uid => timestamp of last message
  private heartbeatTimer: NodeJS.Timeout | undefined;
  private requests: Map<string, Request> = new Map();
  private ackRequests: Map<string, AckRequest> = new Map();

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
    opts: Partial<PostgresAdapterOptions> = {}
  ) {
    super(nsp);
    this.pool = pool;
    this.uid = opts.uid || randomId();
    const channelPrefix = opts.channelPrefix || "socket.io";
    this.channel = `${channelPrefix}#${nsp.name}`;
    this.tableName = opts.tableName || "socket_io_attachments";
    this.requestsTimeout = opts.requestsTimeout || 5000;
    this.heartbeatInterval = opts.heartbeatInterval || 5000;
    this.heartbeatTimeout = opts.heartbeatTimeout || 10000;
    this.payloadThreshold = opts.payloadThreshold || 8000;
    this.errorHandler = opts.errorHandler || defaultErrorHandler;

    this.publish({
      type: EventType.INITIAL_HEARTBEAT,
    });
  }

  close(): Promise<void> | void {
    debug("closing adapter");
    if (this.heartbeatTimer) {
      clearTimeout(this.heartbeatTimer);
    }
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

    debug("new event of type %d from %s", document.type, document.uid);

    if (document.uid && document.uid !== EMITTER_UID) {
      this.nodesMap.set(document.uid, Date.now());
    }

    switch (document.type) {
      case EventType.INITIAL_HEARTBEAT: {
        this.publish({
          type: EventType.HEARTBEAT,
        });
        break;
      }
      case EventType.BROADCAST: {
        debug("broadcast with opts %j", document.data.opts);

        const withAck = document.data.requestId !== undefined;
        if (withAck) {
          super.broadcastWithAck(
            document.data.packet,
            PostgresAdapter.deserializeOptions(document.data.opts),
            (clientCount) => {
              debug("waiting for %d client acknowledgements", clientCount);
              this.publish({
                type: EventType.BROADCAST_CLIENT_COUNT,
                data: {
                  requestId: document.data.requestId,
                  clientCount,
                },
              });
            },
            (arg) => {
              debug("received acknowledgement with value %j", arg);
              this.publish({
                type: EventType.BROADCAST_ACK,
                data: {
                  requestId: document.data.requestId,
                  packet: arg,
                },
              });
            }
          );
        } else {
          super.broadcast(
            document.data.packet,
            PostgresAdapter.deserializeOptions(document.data.opts)
          );
        }
        break;
      }

      case EventType.BROADCAST_CLIENT_COUNT: {
        const request = this.ackRequests.get(document.data.requestId);
        request?.clientCountCallback(document.data.clientCount);
        break;
      }

      case EventType.BROADCAST_ACK: {
        const request = this.ackRequests.get(document.data.requestId);
        request?.ack(document.data.packet);
        break;
      }

      case EventType.SOCKETS_JOIN: {
        debug("calling addSockets with opts %j", document.data.opts);
        super.addSockets(
          PostgresAdapter.deserializeOptions(document.data.opts),
          document.data.rooms
        );
        break;
      }

      case EventType.SOCKETS_LEAVE: {
        debug("calling delSockets with opts %j", document.data.opts);
        super.delSockets(
          PostgresAdapter.deserializeOptions(document.data.opts),
          document.data.rooms
        );
        break;
      }
      case EventType.DISCONNECT_SOCKETS: {
        debug("calling disconnectSockets with opts %j", document.data.opts);
        super.disconnectSockets(
          PostgresAdapter.deserializeOptions(document.data.opts),
          document.data.close
        );
        break;
      }
      case EventType.FETCH_SOCKETS: {
        debug("calling fetchSockets with opts %j", document.data.opts);
        const localSockets = await super.fetchSockets(
          PostgresAdapter.deserializeOptions(document.data.opts)
        );

        this.publish({
          type: EventType.FETCH_SOCKETS_RESPONSE,
          data: {
            requestId: document.data.requestId,
            sockets: localSockets.map((socket) => ({
              id: socket.id,
              handshake: socket.handshake,
              rooms: [...socket.rooms],
              data: socket.data,
            })),
          },
        });
        break;
      }
      case EventType.FETCH_SOCKETS_RESPONSE: {
        const request = this.requests.get(document.data.requestId);

        if (!request) {
          return;
        }

        request.current++;
        document.data.sockets.forEach((socket: any) =>
          request.responses.push(socket)
        );

        if (request.current === request.expected) {
          clearTimeout(request.timeout);
          request.resolve(request.responses);
          this.requests.delete(document.data.requestId);
        }
        break;
      }
      case EventType.SERVER_SIDE_EMIT: {
        const packet = document.data.packet;
        const withAck = document.data.requestId !== undefined;
        if (!withAck) {
          this.nsp._onServerSideEmit(packet);
          return;
        }
        let called = false;
        const callback = (arg: any) => {
          // only one argument is expected
          if (called) {
            return;
          }
          called = true;
          debug("calling acknowledgement with %j", arg);
          this.publish({
            type: EventType.SERVER_SIDE_EMIT_RESPONSE,
            data: {
              requestId: document.data.requestId,
              packet: arg,
            },
          });
        };

        packet.push(callback);
        this.nsp._onServerSideEmit(packet);
        break;
      }
      case EventType.SERVER_SIDE_EMIT_RESPONSE: {
        const request = this.requests.get(document.data.requestId);

        if (!request) {
          return;
        }

        request.current++;
        request.responses.push(document.data.packet);

        if (request.current === request.expected) {
          clearTimeout(request.timeout);
          request.resolve(null, request.responses);
          this.requests.delete(document.data.requestId);
        }
      }
    }
  }

  private scheduleHeartbeat() {
    if (this.heartbeatTimer) {
      clearTimeout(this.heartbeatTimer);
    }
    this.heartbeatTimer = setTimeout(() => {
      this.publish({
        type: EventType.HEARTBEAT,
      });
      this.scheduleHeartbeat();
    }, this.heartbeatInterval);
  }

  private async publish(document: any) {
    document.uid = this.uid;

    try {
      if (
        [
          EventType.BROADCAST,
          EventType.BROADCAST_ACK,
          EventType.SERVER_SIDE_EMIT,
          EventType.SERVER_SIDE_EMIT_RESPONSE,
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

      this.scheduleHeartbeat();
    } catch (err) {
      this.errorHandler(err);
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
    this.pool.query(`SELECT pg_notify($1, $2)`, [this.channel, headerPayload]);
  }

  /**
   * Transform ES6 Set into plain arrays
   */
  private static serializeOptions(opts: BroadcastOptions) {
    return {
      rooms: [...opts.rooms],
      except: opts.except ? [...opts.except] : [],
      flags: opts.flags,
    };
  }

  private static deserializeOptions(opts: any): BroadcastOptions {
    return {
      rooms: new Set(opts.rooms),
      except: new Set(opts.except),
      flags: opts.flags,
    };
  }

  public broadcast(packet: any, opts: BroadcastOptions) {
    const onlyLocal = opts?.flags?.local;
    if (!onlyLocal) {
      this.publish({
        type: EventType.BROADCAST,
        data: {
          packet,
          opts: PostgresAdapter.serializeOptions(opts),
        },
      });
    }

    // packets with binary contents are modified by the broadcast method, hence the nextTick()
    process.nextTick(() => {
      super.broadcast(packet, opts);
    });
  }

  public broadcastWithAck(
    packet: any,
    opts: BroadcastOptions,
    clientCountCallback: (clientCount: number) => void,
    ack: (...args: any[]) => void
  ) {
    const onlyLocal = opts?.flags?.local;
    if (!onlyLocal) {
      const requestId = randomId();

      this.publish({
        type: EventType.BROADCAST,
        data: {
          packet,
          requestId,
          opts: PostgresAdapter.serializeOptions(opts),
        },
      });

      this.ackRequests.set(requestId, {
        type: EventType.BROADCAST,
        clientCountCallback,
        ack,
      });

      // we have no way to know at this level whether the server has received an acknowledgement from each client, so we
      // will simply clean up the ackRequests map after the given delay
      setTimeout(() => {
        this.ackRequests.delete(requestId);
      }, opts.flags!.timeout);
    }

    // packets with binary contents are modified by the broadcast method, hence the nextTick()
    process.nextTick(() => {
      super.broadcastWithAck(packet, opts, clientCountCallback, ack);
    });
  }

  public serverCount(): Promise<number> {
    return Promise.resolve(1 + this.nodesMap.size);
  }

  addSockets(opts: BroadcastOptions, rooms: Room[]) {
    super.addSockets(opts, rooms);

    const onlyLocal = opts.flags?.local;
    if (onlyLocal) {
      return;
    }

    this.publish({
      type: EventType.SOCKETS_JOIN,
      data: {
        opts: PostgresAdapter.serializeOptions(opts),
        rooms,
      },
    });
  }

  delSockets(opts: BroadcastOptions, rooms: Room[]) {
    super.delSockets(opts, rooms);

    const onlyLocal = opts.flags?.local;
    if (onlyLocal) {
      return;
    }

    this.publish({
      type: EventType.SOCKETS_LEAVE,
      data: {
        opts: PostgresAdapter.serializeOptions(opts),
        rooms,
      },
    });
  }

  disconnectSockets(opts: BroadcastOptions, close: boolean) {
    super.disconnectSockets(opts, close);

    const onlyLocal = opts.flags?.local;
    if (onlyLocal) {
      return;
    }

    this.publish({
      type: EventType.DISCONNECT_SOCKETS,
      data: {
        opts: PostgresAdapter.serializeOptions(opts),
        close,
      },
    });
  }

  private getExpectedResponseCount() {
    this.nodesMap.forEach((lastSeen, uid) => {
      const nodeSeemsDown = Date.now() - lastSeen > this.heartbeatTimeout;
      if (nodeSeemsDown) {
        debug("node %s seems down", uid);
        this.nodesMap.delete(uid);
      }
    });
    return this.nodesMap.size;
  }

  async fetchSockets(opts: BroadcastOptions): Promise<any[]> {
    const localSockets = await super.fetchSockets(opts);
    const expectedResponseCount = this.getExpectedResponseCount();

    if (opts.flags?.local || expectedResponseCount === 0) {
      return localSockets;
    }

    const requestId = randomId();

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        const storedRequest = this.requests.get(requestId);
        if (storedRequest) {
          reject(
            new Error(
              `timeout reached: only ${storedRequest.current} responses received out of ${storedRequest.expected}`
            )
          );
          this.requests.delete(requestId);
        }
      }, this.requestsTimeout);

      const storedRequest = {
        type: EventType.FETCH_SOCKETS,
        resolve,
        timeout,
        current: 0,
        expected: expectedResponseCount,
        responses: localSockets,
      };
      this.requests.set(requestId, storedRequest);

      this.publish({
        type: EventType.FETCH_SOCKETS,
        data: {
          opts: PostgresAdapter.serializeOptions(opts),
          requestId,
        },
      });
    });
  }

  public serverSideEmit(packet: any[]): void {
    const withAck = typeof packet[packet.length - 1] === "function";

    if (withAck) {
      this.serverSideEmitWithAck(packet).catch(() => {
        // ignore errors
      });
      return;
    }

    this.publish({
      type: EventType.SERVER_SIDE_EMIT,
      data: {
        packet,
      },
    });
  }

  private async serverSideEmitWithAck(packet: any[]) {
    const ack = packet.pop();
    const expectedResponseCount = this.getExpectedResponseCount();

    debug(
      'waiting for %d responses to "serverSideEmit" request',
      expectedResponseCount
    );

    if (expectedResponseCount <= 0) {
      return ack(null, []);
    }

    const requestId = randomId();

    const timeout = setTimeout(() => {
      const storedRequest = this.requests.get(requestId);
      if (storedRequest) {
        ack(
          new Error(
            `timeout reached: only ${storedRequest.current} responses received out of ${storedRequest.expected}`
          ),
          storedRequest.responses
        );
        this.requests.delete(requestId);
      }
    }, this.requestsTimeout);

    const storedRequest = {
      type: EventType.FETCH_SOCKETS,
      resolve: ack,
      timeout,
      current: 0,
      expected: expectedResponseCount,
      responses: [],
    };
    this.requests.set(requestId, storedRequest);

    this.publish({
      type: EventType.SERVER_SIDE_EMIT,
      data: {
        requestId, // the presence of this attribute defines whether an acknowledgement is needed
        packet,
      },
    });
  }
}

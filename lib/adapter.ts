import { type Pool } from "pg";
import { ClusterAdapterWithHeartbeat } from "socket.io-adapter";
import type {
  ClusterAdapterOptions,
  ClusterMessage,
  ClusterResponse,
  Offset,
  ServerId,
} from "socket.io-adapter";
import debugModule from "debug";
import { PubSubClient } from "./util";

const debug = debugModule("socket.io-postgres-adapter");

export interface PostgresAdapterOptions {
  /**
   * The prefix of the notification channel
   * @default "socket.io"
   */
  channelPrefix?: string;
  /**
   * The name of the table for payloads over the 8000 bytes limit or containing binary data
   * @default "socket_io_attachments"
   */
  tableName?: string;
  /**
   * The threshold for the payload size in bytes (see https://www.postgresql.org/docs/current/sql-notify.html)
   * @default 8000
   */
  payloadThreshold?: number;
  /**
   * Number of ms between two cleanup queries
   * @default 30000
   */
  cleanupInterval?: number;
  /**
   * Handler for errors. If undefined, the errors will be simply logged.
   *
   * @default undefined
   */
  errorHandler?: (err: Error) => void;
}

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
  opts: PostgresAdapterOptions & ClusterAdapterOptions = {}
) {
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

  const namespaces = new Map<string, PostgresAdapter>();
  const client = new PubSubClient(
    pool,
    options,
    (msg) => {
      // @ts-expect-error uid is protected
      return namespaces.get(msg.nsp)?.uid === msg.uid;
    },
    (msg) => {
      namespaces.get(msg.nsp)?.onMessage(msg);
    }
  );

  return function (nsp: any) {
    let adapter = new PostgresAdapter(nsp, opts, client);

    namespaces.set(nsp.name, adapter);
    client.addNamespace(nsp.name);

    const defaultClose = adapter.close;

    adapter.close = () => {
      namespaces.delete(nsp.name);

      if (namespaces.size === 0) {
        client.close();
      }

      defaultClose.call(adapter);
    };

    return adapter;
  };
}

export class PostgresAdapter extends ClusterAdapterWithHeartbeat {
  /**
   * Adapter constructor.
   *
   * @param nsp - the namespace
   * @param opts - additional options
   * @param client
   *
   * @public
   */
  constructor(
    nsp: any,
    opts: ClusterAdapterOptions,
    private readonly client: PubSubClient
  ) {
    super(nsp, opts);
  }

  protected override doPublish(message: ClusterMessage): Promise<Offset> {
    return this.client.publish(message).then(() => {
      // connection state recovery is not currently supported
      return "";
    });
  }

  protected override doPublishResponse(
    _requesterUid: ServerId,
    response: ClusterResponse
  ) {
    return this.client.publish(response);
  }
}

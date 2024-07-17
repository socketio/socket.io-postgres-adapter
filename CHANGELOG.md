# History

| Version                  | Release date  |
|--------------------------|---------------|
| [0.4.0](#040-2024-07-17) | July 2024     |
| [0.3.1](#031-2023-02-08) | February 2023 |
| [0.3.0](#030-2022-04-28) | April 2022    |
| [0.2.0](#020-2021-12-16) | December 2021 |
| [0.1.1](#011-2021-06-28) | June 2021     |
| [0.1.0](#010-2021-06-14) | June 2021     |

# Release notes

## [0.4.0](https://github.com/socketio/socket.io-postgres-adapter/compare/0.3.1...0.4.0) (2024-07-17)

This release contains an important refactor of the adapter ([this commit](https://github.com/socketio/socket.io-postgres-adapter/commit/f783d4c568d232c40e47770e15763e97a77ed338)), as most of the logic has been moved in the `ClusterAdapter` class of the `socket.io-adapter` package.



## [0.3.1](https://github.com/socketio/socket.io-postgres-adapter/compare/0.3.0...0.3.1) (2023-02-08)

The `socket.io-adapter` package was added to the list of `peerDependencies`, in order to fix sync issues with the version imported by the socket.io package (see [d177075](https://github.com/socketio/socket.io-postgres-adapter/commit/d1770759bccba27c7375dbaf89234f4f7dbabc2c)).

Support for connection state recovery (see [here](https://github.com/socketio/socket.io/releases/4.6.0)) will be added in the next release.



## [0.3.0](https://github.com/socketio/socket.io-postgres-adapter/compare/0.2.0...0.3.0) (2022-04-28)


### Features

* broadcast and expect multiple acks ([829a1f5](https://github.com/socketio/socket.io-postgres-adapter/commit/829a1f528df7b723ab6efb0e56248f326bca0c8e))

This feature was added in `socket.io@4.5.0`:

```js
io.timeout(1000).emit("some-event", (err, responses) => {
  // ...
});
```

Thanks to this change, it will now work with multiple Socket.IO servers.

* use a single Postgres connection for all namespaces ([651e281](https://github.com/socketio/socket.io-postgres-adapter/commit/651e28169185d91c7a1a86152d21aa265d5500f2))

The adapter will now create one single Postgres connection for all namespaces, instead of one per namespace, which could lead to performance issues.



## [0.2.0](https://github.com/socketio/socket.io-postgres-adapter/compare/0.1.1...0.2.0) (2021-12-16)


### Features

* add errorHandler option ([#5](https://github.com/socketio/socket.io-postgres-adapter/issues/5)) ([ec1b78c](https://github.com/socketio/socket.io-postgres-adapter/commit/ec1b78cf132147960f05402f6ae9b75ec77e1dd6))



## [0.1.1](https://github.com/socketio/socket.io-postgres-adapter/compare/0.1.0...0.1.1) (2021-06-28)


### Bug Fixes

* prevent SQL injection in the NOTIFY payload ([#1](https://github.com/socketio/socket.io-postgres-adapter/issues/1)) ([580cec2](https://github.com/socketio/socket.io-postgres-adapter/commit/580cec262f37305f5ae92aca62e2bf1d2f9e1741))


## 0.1.0 (2021-06-14)

Initial commit


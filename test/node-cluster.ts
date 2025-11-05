import { fork } from "node:child_process";
import { join } from "node:path";
import assert from "node:assert";
import { times } from "./util.ts";

describe("@socket.io/postgres-adapter within Node.js cluster", () => {
  it("should work", function (done) {
    this.timeout(5000);
    const PRIMARY_COUNT = 3;
    const partialDone = times(PRIMARY_COUNT, done);

    for (let i = 1; i <= PRIMARY_COUNT; i++) {
      const worker = fork(join(__dirname, "fixtures/primary.ts"));

      worker.on("exit", (code) => {
        assert.equal(code, 0);

        partialDone();
      });
    }
  });
});

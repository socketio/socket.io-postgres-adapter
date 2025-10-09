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

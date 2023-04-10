export function times(count: number, fn: () => void) {
  let i = 0;
  return () => {
    i++;
    if (i === count) {
      fn();
    } else if (i > count) {
      throw new Error(`too many calls: ${i} instead of ${count}`);
    }
  };
}

export function sleep(duration: number) {
  return new Promise((resolve) => setTimeout(resolve, duration));
}

export function shouldNotHappen(done) {
  return () => done(new Error("should not happen"));
}

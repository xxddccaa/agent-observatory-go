// Node 25 exposes a non-functional globalThis.localStorage stub when
// --localstorage-file is not set, shadowing Vitest's jsdom Storage methods.
function ensureWorkingLocalStorage(): void {
  if (typeof globalThis.localStorage?.clear === "function") {
    return;
  }
  const store = new Map<string, string>();
  const memoryStorage: Storage = {
    get length() {
      return store.size;
    },
    clear() {
      store.clear();
    },
    getItem(key: string) {
      return store.has(key) ? store.get(key)! : null;
    },
    key(index: number) {
      return [...store.keys()][index] ?? null;
    },
    removeItem(key: string) {
      store.delete(key);
    },
    setItem(key: string, value: string) {
      store.set(key, String(value));
    },
  };
  Object.defineProperty(globalThis, "localStorage", {
    configurable: true,
    writable: true,
    value: memoryStorage,
  });
  if (typeof window !== "undefined") {
    Object.defineProperty(window, "localStorage", {
      configurable: true,
      writable: true,
      value: memoryStorage,
    });
  }
}

ensureWorkingLocalStorage();

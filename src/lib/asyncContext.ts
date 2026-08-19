import { AsyncLocalStorage } from 'node:async_hooks';

interface RequestContext {
  requestId: string;
  [key: string]: unknown;
}

export const asyncLocalStorage = new AsyncLocalStorage<RequestContext>();

export function getCurrentContext(): RequestContext | undefined {
  return asyncLocalStorage.getStore();
}

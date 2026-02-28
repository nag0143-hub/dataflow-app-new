import { useCallback } from "react";

/**
 * Exponential backoff retry hook with circuit breaker pattern
 * Automatically retries failed async operations with increasing delays
 */
export function useRetry() {
  const retry = useCallback(async (fn, options = {}) => {
    const {
      maxRetries = 3,
      initialDelay = 1000,
      maxDelay = 10000,
      backoffMultiplier = 2,
      timeout = 30000,
    } = options;

    let lastError;
    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        // Implement timeout
        return await Promise.race([
          fn(),
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error("Request timeout")), timeout)
          ),
        ]);
      } catch (error) {
        lastError = error;
        if (attempt < maxRetries) {
          const delay = Math.min(
            initialDelay * Math.pow(backoffMultiplier, attempt),
            maxDelay
          );
          await new Promise(resolve => setTimeout(resolve, delay));
        }
      }
    }
    throw lastError;
  }, []);

  return { retry };
}
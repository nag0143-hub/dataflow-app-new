/**
 * LRU Cache Manager for ColumnMapper
 * 
 * Optimizes memory usage by implementing:
 * - Least Recently Used (LRU) eviction policy
 * - Configurable size limits
 * - Optional TTL (Time-To-Live) support
 * - Cache statistics for monitoring
 */

export class LRUCache {
  constructor(maxSize = 100, ttlMs = null) {
    this.maxSize = maxSize;
    this.ttlMs = ttlMs;
    this.cache = new Map();
    this.accessOrder = [];
    this.stats = {
      hits: 0,
      misses: 0,
      evictions: 0,
    };
  }

  /**
   * Get value from cache
   * @param {string} key - Cache key (e.g., "schema.table")
   * @returns {*} Cached value or undefined
   */
  get(key) {
    const entry = this.cache.get(key);
    
    if (!entry) {
      this.stats.misses++;
      return undefined;
    }

    // Check TTL if enabled
    if (this.ttlMs && Date.now() - entry.timestamp > this.ttlMs) {
      this.cache.delete(key);
      this.accessOrder = this.accessOrder.filter(k => k !== key);
      this.stats.misses++;
      return undefined;
    }

    // Update LRU order (move to end = most recently used)
    this.accessOrder = this.accessOrder.filter(k => k !== key);
    this.accessOrder.push(key);
    this.stats.hits++;

    return entry.value;
  }

  /**
   * Set value in cache
   * @param {string} key - Cache key
   * @param {*} value - Value to cache
   */
  set(key, value) {
    // Remove if already exists to update access order
    if (this.cache.has(key)) {
      this.accessOrder = this.accessOrder.filter(k => k !== key);
    }

    // Add or update entry
    this.cache.set(key, {
      value,
      timestamp: Date.now(),
    });
    this.accessOrder.push(key);

    // Evict least recently used if size exceeded
    if (this.cache.size > this.maxSize) {
      const lruKey = this.accessOrder.shift();
      this.cache.delete(lruKey);
      this.stats.evictions++;
    }
  }

  /**
   * Clear entire cache
   */
  clear() {
    this.cache.clear();
    this.accessOrder = [];
  }

  /**
   * Get cache statistics
   * @returns {Object} Hit rate, miss rate, eviction count
   */
  getStats() {
    const total = this.stats.hits + this.stats.misses;
    return {
      ...this.stats,
      size: this.cache.size,
      hitRate: total > 0 ? ((this.stats.hits / total) * 100).toFixed(2) + '%' : 'N/A',
      maxSize: this.maxSize,
    };
  }

  /**
   * Reset statistics
   */
  resetStats() {
    this.stats = { hits: 0, misses: 0, evictions: 0 };
  }
}

// Singleton instance for ColumnMapper
export const columnCacheManager = new LRUCache(100); // Max 100 table schemas

/**
 * Hook for monitoring cache efficiency
 * @returns {Object} Current cache statistics
 */
export function useCacheStats() {
  return columnCacheManager.getStats();
}

/**
 * Cache invalidation utility
 * Call when selectedObjects change to clear related cache entries
 * @param {Array} selectedObjects - Array of {schema, table} objects
 */
export function invalidateCacheForObjects(selectedObjects) {
  if (!selectedObjects || selectedObjects.length === 0) {
    columnCacheManager.clear();
    return;
  }

  // Invalidate only affected table schemas
  const keysToInvalidate = selectedObjects.map(obj => `${obj.schema}.${obj.table}`);
  keysToInvalidate.forEach(key => {
    columnCacheManager.cache.delete(key);
    columnCacheManager.accessOrder = columnCacheManager.accessOrder.filter(k => k !== key);
  });
}
/**
 * Data indexing utility for O(1) lookups instead of O(n) array.find() calls
 */

export function createIndex(items, keyField) {
  const index = new Map();
  items.forEach(item => {
    if (item[keyField]) {
      index.set(item[keyField], item);
    }
  });
  return index;
}

export function groupBy(items, keyField) {
  const groups = new Map();
  items.forEach(item => {
    const key = item[keyField];
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(item);
  });
  return groups;
}
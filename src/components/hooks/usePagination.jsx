import { useState, useCallback } from "react";

/**
 * Server-side pagination hook
 * Handles cursor-based pagination for large datasets
 */
export function usePagination(pageSize = 50) {
  const [cursor, setCursor] = useState(null);
  const [hasNextPage, setHasNextPage] = useState(false);
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(false);

  const fetchPage = useCallback(async (fetcher, options = {}) => {
    const { reset = false } = options;
    
    try {
      setLoading(true);
      const result = await fetcher({
        cursor: reset ? null : cursor,
        limit: pageSize,
      });

      if (reset) {
        setData(result.items);
        setCursor(null);
      } else {
        setData(prev => [...prev, ...result.items]);
      }
      
      setCursor(result.nextCursor);
      setHasNextPage(!!result.nextCursor);
      return result;
    } finally {
      setLoading(false);
    }
  }, [cursor, pageSize]);

  const reset = useCallback(() => {
    setData([]);
    setCursor(null);
    setHasNextPage(false);
  }, []);

  return {
    data,
    cursor,
    hasNextPage,
    loading,
    fetchPage,
    reset,
  };
}
import { useCallback, useEffect, useRef, useState } from "react";

/**
 * Stale-while-revalidate caching hook
 * Returns cached data immediately while fetching fresh data in background
 */
export function useCache(key, fetcher, options = {}) {
  const { staleTime = 5 * 60 * 1000, cacheTime = 30 * 60 * 1000 } = options;
  const cacheRef = useRef(new Map());
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [revision, setRevision] = useState(0);
  const fetcherRef = useRef(fetcher);
  fetcherRef.current = fetcher;

  const revalidate = useCallback(() => {
    cacheRef.current.delete(key);
    setRevision(r => r + 1);
  }, [key]);

  useEffect(() => {
    const cached = cacheRef.current.get(key);
    const now = Date.now();

    if (cached && now - cached.timestamp < staleTime) {
      setData(cached.data);
      setLoading(false);
      return;
    }

    let isMounted = true;
    setLoading(true);

    fetcherRef.current()
      .then(freshData => {
        if (isMounted) {
          cacheRef.current.set(key, {
            data: freshData,
            timestamp: Date.now(),
          });
          setData(freshData);
          setError(null);
        }
      })
      .catch(err => {
        if (isMounted) {
          setError(err);
          if (cached) setData(cached.data);
        }
      })
      .finally(() => {
        if (isMounted) setLoading(false);
      });

    const cleanup = () => {
      if (cacheRef.current.get(key)?.timestamp < now - cacheTime) {
        cacheRef.current.delete(key);
      }
    };
    const timer = setTimeout(cleanup, cacheTime);

    return () => {
      isMounted = false;
      clearTimeout(timer);
    };
  }, [key, revision, staleTime, cacheTime]);

  return { data, loading, error, revalidate };
}
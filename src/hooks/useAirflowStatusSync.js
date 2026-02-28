import { useEffect, useRef } from "react";

const POLL_INTERVAL_MS = 2 * 60 * 1000;

export default function useAirflowStatusSync(onSynced) {
  const intervalRef = useRef(null);
  const callbackRef = useRef(onSynced);
  callbackRef.current = onSynced;

  useEffect(() => {
    const sync = async () => {
      try {
        const resp = await fetch("/api/airflow/sync-pipeline-status", { method: "POST" });
        if (!resp.ok) return;
        const data = await resp.json();
        if (data.synced > 0 && callbackRef.current) {
          callbackRef.current(data);
        }
      } catch (err) {
        // Silently ignore — Airflow may not be configured
      }
    };

    sync();
    intervalRef.current = setInterval(sync, POLL_INTERVAL_MS);

    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, []);
}

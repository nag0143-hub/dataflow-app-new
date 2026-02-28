import { useState, useEffect } from "react";
import { dataflow } from '@/api/client';

/**
 * Returns the current user and a helper to scope entity queries to this user's tenant.
 * Usage:
 *   const { user, tenantFilter } = useTenant();
 *   dataflow.entities.Connection.filter(tenantFilter)
 *
 * For non-admin roles, records are scoped by `created_by`.
 * This also returns a `scope` helper to filter lists client-side for safety.
 */
export function useTenant() {
  const [user, setUser] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    dataflow.auth.me()
      .then(u => { setUser(u); setLoading(false); })
      .catch(() => setLoading(false));
  }, []);

  // Scope a list of records to the current user
  const scope = (records) => {
    if (!user) return records;
    // admins can see everything; regular users only see their own
    if (user.role === "admin") return records;
    return records.filter(r => r.created_by === user.email);
  };

  return { user, loading, scope };
}
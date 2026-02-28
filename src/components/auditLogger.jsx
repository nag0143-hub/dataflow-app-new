import { dataflow } from '@/api/client';

/**
 * Log user actions to audit trail
 * @param {string} action - create, update, delete, execute, pause, resume, clone
 * @param {string} entityType - connection, job, user, prerequisite, catalog_entry
 * @param {string} entityId - ID of the entity
 * @param {string} entityName - Name of the entity
 * @param {object} changes - { before: {}, after: {} }
 * @param {object} metadata - Additional context
 */
export async function logAudit(action, entityType, entityId, entityName, changes = null, metadata = {}) {
  try {
    const user = await dataflow.auth.me();
    
    await dataflow.entities.AuditLog.create({
      action,
      entity_type: entityType,
      entity_id: entityId,
      entity_name: entityName,
      changes,
      user_email: user.email,
      user_name: user.full_name || user.email,
      metadata
    });
  } catch (error) {
    console.error("Failed to log audit:", error);
  }
}
import { z } from 'zod';

export const loginSchema = z.object({
  username: z.string().min(1, 'Username is required').max(128).trim(),
  password: z.string().min(1, 'Password is required').max(256),
});

export const createEntitySchema = z.object({
  name: z.string().max(255).optional(),
  created_by: z.string().max(128).optional(),
}).passthrough();

export const updateEntitySchema = z.object({}).passthrough();

export const entityParamsSchema = z.object({
  entityName: z.string().min(1).max(64).regex(/^[A-Za-z_]+$/, 'Invalid entity name'),
});

export const entityIdParamsSchema = z.object({
  entityName: z.string().min(1).max(64).regex(/^[A-Za-z_]+$/, 'Invalid entity name'),
  id: z.string().regex(/^\d+$/, 'ID must be numeric'),
});

export const filterBodySchema = z.object({
  query: z.record(z.any()).optional().default({}),
  sort: z.string().max(64).optional(),
  limit: z.number().int().min(1).max(1000).optional(),
  skip: z.number().int().min(0).optional().default(0),
});

export const batchBodySchema = z.object({
  items: z.array(z.record(z.any())).min(1, 'At least one item required').max(100, 'Batch limited to 100 items'),
});

export const workspaceCreateSchema = z.object({
  name: z.string().min(1, 'Workspace name is required').max(128).trim(),
  description: z.string().max(512).optional().default(''),
});

export const workspaceUpdateSchema = z.object({
  name: z.string().min(1).max(128).trim().optional(),
  description: z.string().max(512).optional(),
});

export const switchWorkspaceSchema = z.object({
  workspace_id: z.number().int().positive('Invalid workspace ID'),
});

export const introspectSchemaBody = z.object({
  connectionId: z.union([z.string().regex(/^\d+$/), z.number().int().positive()]),
});

export const gitlabStatusSchema = z.object({
  username: z.string().min(1, 'Username required').max(128),
  password: z.string().min(1, 'Password required').max(256),
});

export const gitlabCommitSchema = z.object({
  username: z.string().min(1).max(128),
  password: z.string().min(1).max(256),
  branch: z.string().max(128).optional(),
  commitMessage: z.string().max(500).optional(),
  files: z.array(z.object({
    path: z.string().min(1).max(512),
    content: z.string(),
  })).min(1, 'At least one file required'),
});

export const purgLogsSchema = z.object({
  days: z.number().int().min(1).max(365).optional().default(30),
});

export const testConnectionSchema = z.object({
  platform: z.string().min(1).max(64).optional(),
  host: z.string().max(512).optional(),
  port: z.number().int().min(1).max(65535).optional(),
  database: z.string().max(256).optional(),
  username: z.string().max(256).optional(),
  password: z.string().max(1024).optional(),
}).passthrough();

export function validate(schema, source = 'body') {
  return (req, res, next) => {
    const data = source === 'params' ? req.params : req.body;
    const result = schema.safeParse(data);
    if (!result.success) {
      const errors = result.error.issues.map(issue => ({
        field: issue.path.join('.'),
        message: issue.message,
      }));
      return res.status(400).json({
        error: 'Validation failed',
        details: errors,
      });
    }
    if (source === 'params') {
      req.validatedParams = result.data;
    } else {
      req.validatedBody = result.data;
    }
    next();
  };
}

export function validateParams(schema) {
  return validate(schema, 'params');
}

export function validateBody(schema) {
  return validate(schema, 'body');
}

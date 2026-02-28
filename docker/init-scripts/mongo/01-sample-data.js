db = db.getSiblingDB('source_db');

db.createUser({
  user: 'source_user',
  pwd: 'source_pass',
  roles: [{ role: 'readWrite', db: 'source_db' }]
});

db.events.insertMany([
  { event_type: 'page_view', user_id: 'u001', page: '/dashboard', timestamp: new Date('2026-02-01T10:00:00Z'), session_id: 'sess_001', device: 'desktop', browser: 'Chrome' },
  { event_type: 'click', user_id: 'u001', page: '/dashboard', element: 'btn_export', timestamp: new Date('2026-02-01T10:05:00Z'), session_id: 'sess_001', device: 'desktop', browser: 'Chrome' },
  { event_type: 'page_view', user_id: 'u002', page: '/pipelines', timestamp: new Date('2026-02-01T11:00:00Z'), session_id: 'sess_002', device: 'mobile', browser: 'Safari' },
  { event_type: 'form_submit', user_id: 'u003', page: '/connections/new', form: 'connection_form', timestamp: new Date('2026-02-01T12:30:00Z'), session_id: 'sess_003', device: 'desktop', browser: 'Firefox' },
  { event_type: 'page_view', user_id: 'u004', page: '/pipelines/1', timestamp: new Date('2026-02-02T09:00:00Z'), session_id: 'sess_004', device: 'tablet', browser: 'Chrome' },
  { event_type: 'api_call', user_id: 'u002', endpoint: '/api/entities/Pipeline', method: 'GET', status_code: 200, timestamp: new Date('2026-02-02T09:15:00Z'), session_id: 'sess_005', device: 'desktop', browser: 'Chrome' },
  { event_type: 'error', user_id: 'u005', page: '/deploy', error_code: 'CONN_TIMEOUT', message: 'Connection timed out', timestamp: new Date('2026-02-02T14:00:00Z'), session_id: 'sess_006', device: 'desktop', browser: 'Edge' },
  { event_type: 'page_view', user_id: 'u001', page: '/connections', timestamp: new Date('2026-02-03T08:00:00Z'), session_id: 'sess_007', device: 'desktop', browser: 'Chrome' }
]);

db.sensor_readings.insertMany([
  { sensor_id: 'temp_001', location: 'Building A Floor 1', value: 72.5, unit: 'F', timestamp: new Date('2026-02-01T00:00:00Z'), quality: 'good' },
  { sensor_id: 'temp_001', location: 'Building A Floor 1', value: 71.8, unit: 'F', timestamp: new Date('2026-02-01T01:00:00Z'), quality: 'good' },
  { sensor_id: 'temp_002', location: 'Building A Floor 2', value: 73.1, unit: 'F', timestamp: new Date('2026-02-01T00:00:00Z'), quality: 'good' },
  { sensor_id: 'humid_001', location: 'Building A Floor 1', value: 45.2, unit: '%', timestamp: new Date('2026-02-01T00:00:00Z'), quality: 'good' },
  { sensor_id: 'power_001', location: 'Building A Main', value: 245.7, unit: 'kW', timestamp: new Date('2026-02-01T00:00:00Z'), quality: 'suspect' },
  { sensor_id: 'temp_001', location: 'Building A Floor 1', value: 68.9, unit: 'F', timestamp: new Date('2026-02-01T02:00:00Z'), quality: 'good' }
]);

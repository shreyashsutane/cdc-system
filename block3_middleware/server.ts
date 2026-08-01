// ==============================================================================
// BLOCK 3: REAL-TIME STREAMING MIDDLEWARE (NODE.JS + SSE)
// ==============================================================================

/**
 * PRODUCTION-GRADE SERVICE ACCOUNT REQUIREMENTS:
 * 
 * To execute queries against the BigQuery analytics ledger, the service account
 * running this middleware gateway must be bound with the following IAM roles:
 *   1. roles/bigquery.dataViewer (To view data and read schemas in cdc_logging dataset)
 *   2. roles/bigquery.jobUser (To run query jobs in the GCP Project)
 */

import express, { Request, Response } from 'express';
import cors from 'cors';
import { BigQuery } from '@google-cloud/bigquery';
import dotenv from 'dotenv';
import path from 'path';

dotenv.config();

const app = express();
const PORT = process.env.PORT || 8080;

app.use(cors());
app.use(express.json());

// Initialize BigQuery Client
// The library automatically resolves credentials using Application Default Credentials (ADC)
// from the local system or the GCP environment.
const bigquery = new BigQuery();

let projectId = '';
async function resolveProjectId() {
  try {
    projectId = await bigquery.getProjectId();
    console.log(`Resolved GCP Project ID: ${projectId}`);
  } catch (error) {
    console.error('Failed to resolve GCP Project ID. Falling back to env variables:', error);
    projectId = process.env.GCP_PROJECT || process.env.GOOGLE_CLOUD_PROJECT || 'unknown-project';
  }
}

// Registry of open client connections for SSE
const clients = new Set<Response>();

// Sliding window deduplication cache for event IDs
const seenEventIds = new Set<string>();
const seenEventIdsQueue: string[] = [];

/**
 * Pushes event ID to deduplication sliding cache and clears out old entries.
 */
function addSeenEventId(id: string) {
  if (seenEventIds.has(id)) return;
  seenEventIds.add(id);
  seenEventIdsQueue.push(id);
  
  // Maintain a cache boundary to protect against memory leaks
  if (seenEventIdsQueue.length > 2000) {
    const oldestId = seenEventIdsQueue.shift();
    if (oldestId) {
      seenEventIds.delete(oldestId);
    }
  }
}

/**
 * Utility to standardise raw BigQuery fields and convert nested JSON fields safely.
 */
function formatBigQueryRow(row: any): any {
  return {
    event_id: row.event_id,
    execution_time: row.execution_time?.value || row.execution_time,
    operation_type: row.operation_type,
    entity_kind: row.entity_kind,
    entity_id: row.entity_id,
    changed_by: row.changed_by,
    old_value: typeof row.old_value === 'string' ? JSON.parse(row.old_value) : row.old_value,
    new_value: typeof row.new_value === 'string' ? JSON.parse(row.new_value) : row.new_value,
  };
}

// Read access token from environment — REQUIRED in production. Set DASHBOARD_ACCESS_TOKEN env var in Cloud Run.
const ACCESS_TOKEN = process.env.DASHBOARD_ACCESS_TOKEN;

/**
 * Authentication Middleware
 * Validates request authorization via Authorization header or ?token query param (for SSE connection support)
 */
function authenticate(req: Request, res: Response, next: () => void) {
  const token = req.header('Authorization')?.split(' ')[1] || req.query.token;
  if (token === ACCESS_TOKEN) {
    return next();
  }
  
  console.warn(`Unauthorized access attempt from IP: ${req.ip}`);
  res.status(401).json({ error: 'Unauthorized: Invalid access token' });
}

// ------------------------------------------------------------------------------
// AUTH VERIFY ENDPOINT
// ------------------------------------------------------------------------------
app.get('/api/auth/verify', authenticate, (req: Request, res: Response) => {
  res.json({ status: 'ok' });
});

// ------------------------------------------------------------------------------
// SSE ENDPOINT
// ------------------------------------------------------------------------------
app.get('/api/logs/stream', authenticate, async (req: Request, res: Response) => {
  // Establish Server-Sent Events headers
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    // Prevent intermediate proxy/load-balancer buffering (e.g. GCLB, Nginx)
    'X-Accel-Buffering': 'no',
    'Access-Control-Allow-Origin': '*'
  });

  // Write immediate blank comment to flush headers and establish stream handshake
  res.write(': handshake\n\n');

  console.log(`New client connected. Total clients: ${clients.size + 1}`);

  try {
    // 1. Instantly seed client with historical data (Last 100 mutations)
    const query = `
      SELECT 
        event_id, 
        execution_time, 
        operation_type, 
        entity_kind, 
        entity_id, 
        changed_by, 
        TO_JSON_STRING(old_value) as old_value, 
        TO_JSON_STRING(new_value) as new_value
      FROM \`${projectId}.cdc_logging.datastore_mutations_ledger\`
      ORDER BY execution_time DESC
      LIMIT 100
    `;

    const [rows] = await bigquery.query({ query });
    const historicalLogs = rows.map(formatBigQueryRow);

    // Warm up the local deduplication cache with seed entries
    historicalLogs.forEach((log: any) => addSeenEventId(log.event_id));

    // Send historical payload to client
    res.write(`data: ${JSON.stringify({ type: 'seed', logs: historicalLogs })}\n\n`);
  } catch (error) {
    console.error('Error fetching historical seed records:', error);
    // Send empty seed if BQ query fails (e.g. table is not initialized yet)
    res.write(`data: ${JSON.stringify({ type: 'seed', logs: [] })}\n\n`);
  }

  // Register client
  clients.add(res);

  // Setup periodic client keep-alive pulse (heartbeat)
  const heartbeatInterval = setInterval(() => {
    res.write(': keepalive\n\n');
  }, 15000);

  // Cleanup on client disconnect
  req.on('close', () => {
    clearInterval(heartbeatInterval);
    clients.delete(res);
    console.log(`Client disconnected. Total clients: ${clients.size}`);
  });
});

// ------------------------------------------------------------------------------
// REAL-TIME POLLING ENGINE
// ------------------------------------------------------------------------------
async function pollBigQueryLedger() {
  if (clients.size === 0) {
    // Skip polling query execution if no active frontend displays are connected
    return;
  }

  try {
    // Query records created within the last 5 seconds to ensure overlaps capture everything
    const query = `
      SELECT 
        event_id, 
        execution_time, 
        operation_type, 
        entity_kind, 
        entity_id, 
        changed_by, 
        TO_JSON_STRING(old_value) as old_value, 
        TO_JSON_STRING(new_value) as new_value
      FROM \`${projectId}.cdc_logging.datastore_mutations_ledger\`
      WHERE execution_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 SECOND)
      ORDER BY execution_time ASC
    `;

    const [rows] = await bigquery.query({ query });
    const newLogs: any[] = [];

    for (const row of rows) {
      if (!seenEventIds.has(row.event_id)) {
        addSeenEventId(row.event_id);
        newLogs.push(formatBigQueryRow(row));
      }
    }

    // Broadcast discovered mutation entries to all streaming connections
    if (newLogs.length > 0) {
      console.log(`Broadcasting ${newLogs.length} new log(s) to ${clients.size} client(s).`);
      const payload = JSON.stringify({ type: 'mutation', logs: newLogs });
      for (const clientResponse of clients) {
        clientResponse.write(`data: ${payload}\n\n`);
      }
    }
  } catch (error) {
    console.error('Error running polling query on BigQuery:', error);
  }
}

// ------------------------------------------------------------------------------
// APP BOOTSTRAP
// ------------------------------------------------------------------------------
async function bootstrap() {
  await resolveProjectId();

  // Run polling engine every 1000ms
  setInterval(pollBigQueryLedger, 1000);

  // Serve React static files in production container
  const staticPath = path.join(__dirname, 'public');
  app.use(express.static(staticPath));

  // Catch-all to support SPA routing (redirect non-API requests to index.html)
  app.get('*', (req, res, next) => {
    if (req.path.startsWith('/api')) {
      return next();
    }
    res.sendFile(path.join(staticPath, 'index.html'));
  });

  app.listen(PORT, () => {
    console.log(`CDC Streaming Middleware running at http://localhost:${PORT}`);
    console.log(`SSE streaming endpoint available at: http://localhost:${PORT}/api/logs/stream`);
  });
}

bootstrap();

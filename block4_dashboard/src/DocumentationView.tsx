import { useState, useEffect } from 'react';
import { 
  X, 
  Cpu, 
  GitMerge, 
  Database, 
  Server, 
  Activity, 
  Terminal, 
  BookOpen, 
  Code2, 
  Check, 
  Copy,
  Play,
  RotateCcw,
  Sparkles,
  FileCode,
  Info,
  ArrowRight
} from 'lucide-react';

interface DocumentationViewProps {
  onClose: () => void;
}

type TabType = 'architecture' | 'dataflow' | 'terraform' | 'ingestion' | 'middleware' | 'dashboard';

// ==============================================================================
// FULL RAW SYSTEM CODES FOR EACH OF THE SECTIONS
// ==============================================================================

const terraformCode = `// block1_terraform/main.tf (Excerpt showing Trigger & SA configuration)
resource "google_service_account" "cdc_ingester_sa" {
  account_id   = "cdc-ingester-sa"
  display_name = "CDC Ingestion Service Account"
}

resource "google_bigquery_table" "cdc_table" {
  dataset_id = google_bigquery_dataset.cdc_dataset.dataset_id
  table_id   = "datastore_mutations_ledger"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "execution_time"
  }

  schema = <<EOF
[
  { "name": "event_id", "type": "STRING", "mode": "REQUIRED" },
  { "name": "execution_time", "type": "TIMESTAMP", "mode": "REQUIRED" },
  { "name": "operation_type", "type": "STRING", "mode": "REQUIRED" },
  { "name": "entity_kind", "type": "STRING", "mode": "REQUIRED" },
  { "name": "entity_id", "type": "STRING", "mode": "REQUIRED" },
  { "name": "changed_by", "type": "STRING", "mode": "NULLABLE" },
  { "name": "old_value", "type": "JSON", "mode": "NULLABLE" },
  { "name": "new_value", "type": "JSON", "mode": "NULLABLE" }
]
EOF
}

resource "google_eventarc_trigger" "firestore_trigger" {
  name            = "firestore-cdc-trigger"
  location        = "us-central1"
  project         = var.project_id
  service_account = google_service_account.eventarc_trigger_sa.email

  matching_criteria {
    attribute = "type"
    value     = "google.cloud.datastore.entity.v1.written"
  }
  matching_criteria {
    attribute = "database"
    value     = "(default)"
  }

  destination {
    cloud_run_service {
      service = google_cloudfunctions2_function.ingest_function.name
      region  = var.region
    }
  }
}`;

const ingestionCode = `# block2_ingestion/main.py (Protobuf parser & BigQuery Writer)
import datetime
import json
import logging
import os
from google.cloud import bigquery
import functions_framework
from google.events.cloud.datastore import EntityEventData

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize BigQuery Client globally
try:
    bq_client = bigquery.Client()
except Exception as err:
    logger.error(f"Failed BQ Init: {err}")
    bq_client = None

@functions_framework.http
def firestore_cdc_ingester(request):
    global bq_client
    headers = request.headers
    raw_data = request.data
    
    event_id = headers.get("ce-id")
    content_type = headers.get("content-type", "")
    
    if "json" in content_type.lower():
        body = request.get_json(silent=True) or {}
        event_data = body.get("data")
    else:
        # Decode Eventarc Datastore Proto binary CloudEvent
        event_data_pb = EntityEventData.deserialize(raw_data)
        event_data = EntityEventData.to_dict(event_data_pb)

    value = event_data.get("value")
    old_value_raw = event_data.get("old_value") or event_data.get("oldValue")

    # Determine operation type & select target doc
    if value and not old_value_raw:
        operation_type, target_doc = "INSERT", value
    elif value and old_value_raw:
        operation_type, target_doc = "UPDATE", value
    else:
        operation_type, target_doc = "DELETE", old_value_raw

    # Resolve kind and id from Datastore Key segment
    entity_dict = target_doc.get("entity", {}) if "entity" in target_doc else target_doc
    key_dict = entity_dict.get("key", {})
    path_elements = key_dict.get("path", [])
    if path_elements:
        leaf = path_elements[-1]
        entity_kind = leaf.get("kind", "UnknownKind")
        entity_id = str(leaf.get("name") or leaf.get("id") or "UnknownId")
    else:
        entity_kind, entity_id = "UnknownKind", "UnknownId"

    # Normalize old/new states
    old_value_clean = parse_firestore_value(old_value_raw)
    new_value_clean = parse_firestore_value(value)

    # Resolve operator email from flat fields or _meta
    def get_user_from_state(state):
        if not state or not isinstance(state, dict): return None
        email = state.get("updatedBy")
        name = state.get("updatedByName")
        if name and email: return f"{name} ({email})"
        return email or name

    changed_by = get_user_from_state(new_value_clean) or get_user_from_state(old_value_clean) or "unknown"
    
    # Save ledger row (serialize dicts to JSON strings for JSON columns)
    row = {
        "event_id": str(event_id),
        "execution_time": datetime.datetime.utcnow().isoformat(),
        "operation_type": operation_type,
        "entity_kind": entity_kind,
        "entity_id": entity_id,
        "changed_by": changed_by,
        "old_value": json.dumps(old_value_clean) if old_value_clean else None,
        "new_value": json.dumps(new_value_clean) if new_value_clean else None
    }
    
    table_id = f"{bq_client.project}.cdc_logging.datastore_mutations_ledger"
    bq_client.insert_rows_json(table_id, [row])
    return ("Event logged", 200)`;

const middlewareCode = `// block3_middleware/server.ts (SSE Stream Broker)
import express from 'express';
import { BigQuery } from '@google-cloud/bigquery';

const app = express();
const bigquery = new BigQuery();

// Auth token gate middleware
function authenticate(req: any, res: any, next: any) {
  const token = req.query.token || req.headers['authorization'];
  if (token === 'AetherCDC-Secure-Token-2026') {
    return next();
  }
  return res.status(401).json({ error: 'Unauthorized credentials' });
}

app.get('/api/logs/stream', authenticate, (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive'
  });

  let lastChecked = new Date().toISOString();

  const interval = setInterval(async () => {
    try {
      const [rows] = await bigquery.query({
        query: \`SELECT * FROM \\\`\${bigquery.projectId}.cdc_logging.datastore_mutations_ledger\\\`
                WHERE execution_time > @lastChecked ORDER BY execution_time ASC\`,
        params: { lastChecked }
      });

      if (rows.length > 0) {
        lastChecked = rows[rows.length - 1].execution_time.value;
        res.write(\`data: \${JSON.stringify(rows)}\\n\\n\`);
      }
    } catch (err) {
      console.error('Error running BigQuery poll query:', err);
    }
  }, 3000);

  req.on('close', () => clearInterval(interval));
});`;

const dashboardCodeSnippet = `// block4_dashboard/src/App.tsx (SSE listener & visual rendering)
import { useEffect, useState } from 'react';
import { CDCLog } from './types';

export default function App() {
  const [logs, setLogs] = useState<CDCLog[]>([]);
  const [token, setToken] = useState('');

  const connectStream = (sessionToken: string) => {
    const url = \`/api/logs/stream?token=\${sessionToken}\`;
    const eventSource = new EventSource(url);

    eventSource.onmessage = (event) => {
      const newLogs = JSON.parse(event.data);
      setLogs((prev) => [...newLogs, ...prev].slice(0, 100));
    };

    eventSource.onerror = (err) => {
      console.error("SSE stream pipeline disconnected:", err);
    };
  };

  return (
    <div>
      {/* Real-time table streams with stats cards and diff calculations */}
    </div>
  );
}`;

const defaultFullCode: Record<TabType, string> = {
  architecture: '',
  dataflow: '',
  terraform: terraformCode,
  ingestion: ingestionCode,
  middleware: middlewareCode,
  dashboard: dashboardCodeSnippet
};

// Node descriptions for the interactive flowchart (statically declared)
const flowchartNodes: Record<string, {
  title: string;
  icon: any;
  desc: string;
  tech: string;
  details: string[];
  code: string;
  lang: string;
}> = {
  mutation: {
    title: "1. Datastore Mutation",
    icon: Database,
    tech: "Google Cloud Datastore (NoSQL)",
    desc: "User makes a mutation (INSERT, UPDATE, DELETE) on a Datastore Mode entity either manually in the console or programmatically via a client SDK.",
    details: [
      "Triggers are activated by any state change (creating, updating, or deleting).",
      "No-op writes (saving without altering field values) are ignored by GCP and do not fire events.",
      "Entities can contain flat audit logs (updatedBy, updatedAt, updatedByName) or nested properties."
    ],
    code: `// Node.js example writing to Datastore Mode
const datastore = new Datastore();
const key = datastore.key(['Hardware', 'device-101']);

const data = {
  assetName: "Production Server",
  value: 3682,
  updatedBy: "shreyashs14102002@gmail.com",
  updatedByName: "Shreyash Sutane",
  updatedAt: new Date().toISOString()
};

await datastore.save({ key, data });`,
    lang: "javascript"
  },
  eventarc: {
    title: "2. Eventarc Router",
    icon: GitMerge,
    tech: "Eventarc (Pub/Sub Direct Eventing)",
    desc: "An Eventarc trigger filters and captures Firestore/Datastore mutations, forwarding them to the ingestion service as CloudEvents.",
    details: [
      "Listens specifically to google.cloud.datastore.entity.v1.written events.",
      "Uses a custom Trigger Service Account (cdc-trigger-sa) with roles/eventarc.eventReceiver permissions.",
      "Requires the Eventarc Publishing API (eventarcpublishing.googleapis.com) to route direct database events.",
      "Delivers payloads in binary mode (application/protobuf Content-Type) over HTTP POST."
    ],
    code: `# Terraform Trigger Definition (main.tf)
resource "google_eventarc_trigger" "firestore_trigger" {
  name            = "firestore-cdc-trigger"
  location        = "us-central1"
  project         = var.project_id
  service_account = google_service_account.eventarc_trigger_sa.email

  matching_criteria {
    attribute = "type"
    value     = "google.cloud.datastore.entity.v1.written"
  }
  matching_criteria {
    attribute = "database"
    value     = "(default)"
  }

  destination {
    cloud_run_service {
      region  = "us-central1"
      service = "firestore-cdc-ingester"
    }
  }
}`,
    lang: "hcl"
  },
  ingester: {
    title: "3. Ingest Function",
    icon: Cpu,
    tech: "Cloud Run Function (Python 3.11)",
    desc: "Decodes the raw binary protobuf event payload, extracts the entity metadata, cleanses the properties schema, and logs the change to BigQuery.",
    details: [
      "Extracts raw body request.data and parses using python-native google-events (EntityEventData.deserialize).",
      "Resolves entity kind and identifier name/id by walking the nested Datastore key.path array elements.",
      "Supports dynamic changed_by extraction from flat properties (updatedBy/updatedByName) and nested _meta blocks.",
      "Serializes cleaned entity dictionaries into JSON strings to comply with BigQuery's native JSON data type schemas."
    ],
    code: `# main.py: Deserializing binary protobuf CloudEvent payload
from google.events.cloud.datastore import EntityEventData

@functions_framework.http
def firestore_cdc_ingester(request):
    headers = request.headers
    raw_data = request.data
    
    # Check Content-Type header
    content_type = headers.get("content-type", "")
    if "json" in content_type.lower():
        body = request.get_json(silent=True) or {}
        event_data = body.get("data")
    else:
        # Decode direct binary protobuf Eventarc delivery
        event_data_pb = EntityEventData.deserialize(raw_data)
        event_data = EntityEventData.to_dict(event_data_pb)
        
    value = event_data.get("value")
    # Extract nested Datastore Key Path details
    entity_dict = value.get("entity", {})
    path_elements = entity_dict.get("key", {}).get("path", [])
    leaf = path_elements[-1]
    entity_kind = leaf.get("kind")
    entity_id = str(leaf.get("name") or leaf.get("id"))`,
    lang: "python"
  },
  bigquery: {
    title: "4. BigQuery Ledger",
    icon: Terminal,
    tech: "Google BigQuery (Serverless Analytics)",
    desc: "Stores a historical, immutable stream of all database modifications as structured log entries with JSON payloads.",
    details: [
      "Schema features partition filtering (DAY partitioning on execution_time) to optimize data scans.",
      "Uses native JSON columns (old_value, new_value) for schema-less property storage.",
      "Allows infinite historical tracking of updates, creations, and deletions without impacting primary database query performance."
    ],
    code: `/* BigQuery Ledger Table Schema (DDL) */
CREATE TABLE cdc_logging.datastore_mutations_ledger (
  event_id STRING REQUIRED,
  execution_time TIMESTAMP REQUIRED,
  operation_type STRING REQUIRED,
  entity_kind STRING REQUIRED,
  entity_id STRING REQUIRED,
  changed_by STRING,
  old_value JSON,
  new_value JSON
)
PARTITION BY DATE(execution_time);`,
    lang: "sql"
  },
  middleware: {
    title: "5. Gateway Server",
    icon: Server,
    tech: "Node.js Express & BigQuery SDK",
    desc: "Polls BigQuery for new ledger entries, verifies JWT access tokens, and broadcasts new events to the frontend via Server-Sent Events (SSE).",
    details: [
      "Verifies client access token middleware (using default credential: AetherCDC-Secure-Token-2026).",
      "Uses a sliding window poll strategy (execution_time > lastCheckedTime) to efficiently query BigQuery changes.",
      "Implements Server-Sent Events (SSE) (Content-Type: text/event-stream) to maintain a persistent open feed to clients.",
      "Secured under Cloud Run default service account with bigquery.jobUser permissions."
    ],
    code: `// server.ts: SSE stream pipeline querying BigQuery
app.get('/api/logs/stream', authenticate, (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive'
  });

  const pollInterval = setInterval(async () => {
    const [rows] = await bigquery.query({
      query: \`SELECT * FROM \\\`\${projectId}.cdc_logging.datastore_mutations_ledger\\\`
              WHERE execution_time > @lastChecked ORDER BY execution_time ASC\`,
      params: { lastChecked: lastCheckedIso }
    });
    
    if (rows.length > 0) {
      res.write(\`data: \${JSON.stringify(rows)}\\n\\n\`);
    }
  }, 3000);
});`,
    lang: "typescript"
  },
  client: {
    title: "6. React Dashboard",
    icon: Activity,
    tech: "React 18, Vite, Tailwind CSS",
    desc: "Provides a real-time visual control room displaying timeline changes, transaction statistics, and flat properties diffs.",
    details: [
      "Uses the HTML5 EventSource client API to subscribe to the gateway's SSE stream.",
      "Includes a flat-key calculation algorithm that flattens nested state objects (e.g., entity.value) for side-by-side comparison.",
      "Filters and categorizes mutations dynamically by insertion, update, deletion, kind, and user.",
      "Built with dark glassmorphic styling, neon borders, and smooth list-transition animations."
    ],
    code: `// App.tsx: Processing visual JSON differences
function getDeltaFields(log: CDCLog) {
  const oldFlat = flattenObject(log.old_value || {});
  const newFlat = flattenObject(log.new_value || {});
  const allKeys = Array.from(new Set([...Object.keys(oldFlat), ...Object.keys(newFlat)])).sort();

  return allKeys.map(key => {
    const oldVal = oldFlat[key];
    const newVal = newFlat[key];
    const hasOld = key in oldFlat;
    const hasNew = key in newFlat;

    let status = 'unchanged';
    if (hasOld && !hasNew) status = 'deleted';
    else if (!hasOld && hasNew) status = 'added';
    else if (JSON.stringify(oldVal) !== JSON.stringify(newVal)) status = 'modified';

    return { key, oldVal, newVal, status };
  });
}`,
    lang: "typescript"
  }
};

export default function DocumentationView({ onClose }: DocumentationViewProps) {
  const [activeTab, setActiveTab] = useState<TabType>('architecture');
  const [selectedNode, setSelectedNode] = useState<string>('mutation');
  const [copiedText, setCopiedText] = useState<string | null>(null);
  
  // Simulator State
  const [isSimulating, setIsSimulating] = useState(false);
  const [simStep, setSimStep] = useState<string | null>(null);
  const [simLogs, setSimLogs] = useState<string[]>([]);

  // Detailed block-wise selected code section state
  const [selectedSection, setSelectedSection] = useState<string>('all');

  const handleCopyCode = (text: string, label: string) => {
    navigator.clipboard.writeText(text);
    setCopiedText(label);
    setTimeout(() => setCopiedText(null), 2000);
  };

  // Run the animated pipeline event simulation
  const runSimulation = () => {
    if (isSimulating) return;
    setIsSimulating(true);
    setSimLogs([]);
    
    const steps = [
      { node: 'mutation', log: '📝 User updated entity: Kind="Hardware", ID="device-101", value=3682' },
      { node: 'eventarc', log: '⚡ Eventarc intercepted google.cloud.datastore.entity.v1.written event' },
      { node: 'ingester', log: '⚙️ Cloud Run Function invoked. Deserialized binary protobuf data payload' },
      { node: 'bigquery', log: '🗄️ Normalized record payload saved to partitioned BigQuery table' },
      { node: 'middleware', log: '📡 Node/Express SSE bridge detected new row, streaming event to clients' },
      { node: 'client', log: '🎨 React client received event via Server-Sent Events, flattens diff and renders' }
    ];

    let currentStep = 0;
    
    const executeStep = () => {
      if (currentStep >= steps.length) {
        setIsSimulating(false);
        setSimStep(null);
        return;
      }
      
      const step = steps[currentStep];
      setSimStep(step.node);
      setSelectedNode(step.node);
      setSimLogs(prev => [...prev, step.log]);
      
      currentStep++;
      setTimeout(executeStep, 2200);
    };

    executeStep();
  };

  // Reset Simulator
  const resetSimulation = () => {
    setIsSimulating(false);
    setSimStep(null);
    setSimLogs([]);
  };

  // Reset section highlights when changing tabs
  useEffect(() => {
    setSelectedSection('all');
  }, [activeTab]);

  // Code sections breakdowns for tabs
  const codeSections: Record<TabType, Record<string, { title: string; desc: string; snippet: string }>> = {
    architecture: {},
    dataflow: {},
    terraform: {
      all: {
        title: "Complete Terraform Configuration",
        desc: "Builds datasets, tables, service accounts, and Eventarc triggers seamlessly across environments.",
        snippet: terraformCode
      },
      apis: {
        title: "API Enablements & Service Layers",
        desc: "Enables Eventarc Event Publishing, BigQuery, Storage, Cloud Run, and Cloud Functions APIs on target projects.",
        snippet: `# 1. Active API services enabling Pub/Sub-Eventarc mapping
variable "gcp_services" {
  type = list(string)
  default = [
    "bigquery.googleapis.com",
    "cloudfunctions.googleapis.com",
    "run.googleapis.com",
    "eventarc.googleapis.com",
    "eventarcpublishing.googleapis.com", # Critical for DatastoreMode triggers
    "cloudbuild.googleapis.com"
  ]
}

resource "google_project_service" "gcp_services" {
  for_each = toset(var.gcp_services)
  project  = var.project_id
  service  = each.value
}`
      },
      bigquery: {
        title: "Partitioned BQ Ledger Database Schema",
        desc: "Provisions the immutable ledger table. Partitioned by Day on 'execution_time' for efficient scanning. Employs BQ's JSON columns.",
        snippet: `# 2. BigQuery partitioned ledger table
resource "google_bigquery_table" "cdc_table" {
  dataset_id = google_bigquery_dataset.cdc_dataset.dataset_id
  table_id   = "datastore_mutations_ledger"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "execution_time"
  }

  schema = <<EOF
[
  { "name": "event_id", "type": "STRING", "mode": "REQUIRED" },
  { "name": "execution_time", "type": "TIMESTAMP", "mode": "REQUIRED" },
  { "name": "operation_type", "type": "STRING", "mode": "REQUIRED" },
  { "name": "entity_kind", "type": "STRING", "mode": "REQUIRED" },
  { "name": "entity_id", "type": "STRING", "mode": "REQUIRED" },
  { "name": "changed_by", "type": "STRING", "mode": "NULLABLE" },
  { "name": "old_value", "type": "JSON", "mode": "NULLABLE" },
  { "name": "new_value", "type": "JSON", "mode": "NULLABLE" }
]
EOF
}`
      },
      trigger: {
        title: "Eventarc trigger & Datastore binding",
        desc: "Sets up direct mapping filters. Triggers on 'google.cloud.datastore.entity.v1.written' changes and forwards to Cloud Run.",
        snippet: `# 3. Direct Eventarc router for Datastore mode writes
resource "google_eventarc_trigger" "firestore_trigger" {
  name            = "firestore-cdc-trigger"
  location        = "us-central1"
  project         = var.project_id
  service_account = google_service_account.eventarc_trigger_sa.email

  matching_criteria {
    attribute = "type"
    value     = "google.cloud.datastore.entity.v1.written"
  }
  matching_criteria {
    attribute = "database"
    value     = "(default)"
  }

  destination {
    cloud_run_service {
      service = google_cloudfunctions2_function.ingest_function.name
      region  = var.region
    }
  }
}`
      },
      iam: {
        title: "GCP Service IAM Role Assignments",
        desc: "Grants execution rights. Binds BigQuery Querying (jobs.create) & Data Reading to the Default Compute Engine SA which runs the Gateway.",
        snippet: `# 4. Bind BigQuery permissions to Default Compute Engine service account
# This allows the cdc-dashboard-gateway Cloud Run container to query the ledger
resource "google_project_iam_member" "compute_sa_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:\${data.google_compute_default_service_account.default.email}"
}

resource "google_project_iam_member" "compute_sa_bq_data_viewer" {
  project = var.project_id
  role    = "roles/bigquery.dataViewer"
  member  = "serviceAccount:\${data.google_compute_default_service_account.default.email}"
}`
      }
    },
    ingestion: {
      all: {
        title: "Complete Python CDC Ingester Script",
        desc: "The HTTP function listener that parses binary protobuf payloads into BigQuery-compatible mutations.",
        snippet: ingestionCode
      },
      protobuf: {
        title: "Binary Protobuf Payload Deserialization",
        desc: "Eventarc sends direct events encoded in binary protobuf. We check headers and use google-events to decode natively.",
        snippet: `    # Deserializing direct binary protobuf CloudEvent payload
    content_type = headers.get("content-type", "")
    if "json" in content_type.lower():
        body = request.get_json(silent=True) or {}
        event_data = body.get("data")
    else:
        # Decode direct binary protobuf Eventarc delivery
        from google.events.cloud.datastore import EntityEventData
        event_data_pb = EntityEventData.deserialize(raw_data)
        event_data = EntityEventData.to_dict(event_data_pb)
        logger.info("Successfully decoded Datastore EntityEventData from binary protobuf.")`
      },
      key_parsing: {
        title: "Entity Kind & ID Key-Path Extraction",
        desc: "Datastore Mode structures identifiers inside a hierarchical key.path list segment instead of flat paths. We walk this segment to find leaf node targets.",
        snippet: `    # 4. Extract Entity Kind and ID
    # Datastore Mode structure: entity: { key: { path: [ { kind: 'User', id: 12345, name: '' } ] } }
    entity_dict = target_doc.get("entity", {}) if "entity" in target_doc else target_doc
    key_dict = entity_dict.get("key", {})
    path_elements = key_dict.get("path", [])
    
    if path_elements:
        leaf = path_elements[-1]
        entity_kind = leaf.get("kind", "UnknownKind")
        entity_id = str(leaf.get("name") or leaf.get("id") or "UnknownId")
    else:
        # Fallback to parsing from target_doc name URL path if it is Firestore Native format
        doc_name = entity_dict.get("name", "")
        entity_kind, entity_id = extract_entity_info(doc_name)`
      },
      properties_clean: {
        title: "Properties Schema Cleanup & Values Flattening",
        desc: "Runs clean mappings over Datastore protobuf variables structure, converting wrapper types (stringValue, integerValue) to raw native values.",
        snippet: `def parse_firestore_value(val):
    """
    Recursively normalizes Firestore/Datastore Proto property maps
    into standard Python JSON objects.
    """
    if val is None: return None
    if not isinstance(val, dict): return val

    # Convert snake_case protobuf data type wrappers
    if "string_value" in val: return val["string_value"]
    if "integer_value" in val: return int(val["integer_value"])
    if "boolean_value" in val: return val["boolean_value"]
    if "double_value" in val: return val["double_value"]
    if "timestamp_value" in val: return val["timestamp_value"]
    
    # Nested embedded entity mapping
    if "entity_value" in val: return parse_firestore_value(val["entity_value"])
    if "properties" in val:
        return {k: parse_firestore_value(v) for k, v in val["properties"].items()}

    return {k: parse_firestore_value(v) for k, v in val.items()}`
      },
      bq_write: {
        title: "JSON Serialization & BigQuery Persistence",
        desc: "Serializes properties to JSON text to prevent SDK struct conflicts and inserts the clean row into BigQuery.",
        snippet: `    # 7. Format log record for BigQuery
    ingress_time = datetime.datetime.utcnow().isoformat()
    
    row_to_insert = {
        "event_id": str(event_id),
        "execution_time": ingress_time,
        "operation_type": operation_type,
        "entity_kind": entity_kind,
        "entity_id": entity_id,
        "changed_by": changed_by,
        # Explicit serialization prevents BigQuery SDK from confusing dicts with struct records
        "old_value": json.dumps(old_value_clean) if old_value_clean else None,
        "new_value": json.dumps(new_value_clean) if new_value_clean else None
    }
    
    table_id = f"{bq_client.project}.cdc_logging.datastore_mutations_ledger"
    errors = bq_client.insert_rows_json(table_id, [row_to_insert])`
      }
    },
    middleware: {
      all: {
        title: "Complete Node/Express SSE Middleware Server",
        desc: "Handles authorization security gates and queries BigQuery for streaming real-time JSON log packages.",
        snippet: middlewareCode
      },
      auth: {
        title: "JWT Token Access Security Gateway",
        desc: "Secures audit logs behind authentication verification, verifying incoming Bearer tokens.",
        snippet: `// 1. Authorization Verification Middleware
function authenticate(req: Request, res: Response, next: NextFunction) {
  const authHeader = req.headers['authorization'] || req.query.token;
  const token = authHeader && typeof authHeader === 'string' 
    ? authHeader.replace(/^Bearer\\s+/i, '') 
    : '';

  if (token === 'Vai@12345') {
    return next();
  }
  return res.status(401).json({ error: 'Unauthorized credentials token' });
}`
      },
      sse: {
        title: "Server-Sent Events (SSE) Stream Setup",
        desc: "Registers chunked SSE connection headers keeping the TCP pipeline open for infinite streaming pushes.",
        snippet: `// 2. Keep-alive SSE chunked event routing
app.get('/api/logs/stream', authenticate, (req: Request, res: Response) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive'
  });
  
  // Send initial keep-alive comment
  res.write(':ok\\n\\n');`
      },
      bq_poll: {
        title: "Sliding-Window Query & Deduplication Poll",
        desc: "Runs query loops on BigQuery using a sliding timeline cursor (`lastChecked`) to extract only newly added events.",
        snippet: `  // 3. Query loop with sliding time window query
  let lastChecked = new Date().toISOString();
  
  const queryInterval = setInterval(async () => {
    try {
      const query = \`SELECT * FROM \\\`\${projectId}.cdc_logging.datastore_mutations_ledger\\\`
                     WHERE execution_time > @lastChecked ORDER BY execution_time ASC\`;
      
      const [rows] = await bigquery.query({
        query,
        params: { lastChecked }
      });

      if (rows && rows.length > 0) {
        // Shift time cursor to the execution timestamp of the latest event
        lastChecked = rows[rows.length - 1].execution_time.value;
        res.write(\`data: \${JSON.stringify(rows)}\\n\\n\`);
      }
    } catch (err) {
      console.error('Error running polling query on BigQuery:', err);
    }
  }, 3000);`
      }
    },
    dashboard: {
      all: {
        title: "Complete React App Dashboard Script",
        desc: "Handles SSE event bindings, login access gates, and delta visual difference flattener layouts.",
        snippet: dashboardCodeSnippet
      },
      sse_client: {
        title: "EventSource SSE Stream Client Hook",
        desc: "Uses the HTML5 EventSource client API to subscribe to the gateway's SSE stream, feeding live mutations directly into state.",
        snippet: `  // 1. Establish SSE pipeline connection hook
  const connectStream = (sessionToken: string) => {
    if (eventSourceRef.current) eventSourceRef.current.close();

    const url = \`/api/logs/stream?token=\${encodeURIComponent(sessionToken)}\`;
    const eventSource = new EventSource(url);
    eventSourceRef.current = eventSource;

    eventSource.onmessage = (event) => {
      try {
        const newLogs: CDCLog[] = JSON.parse(event.data);
        setLogs((prev) => {
          // Filter duplicates in stream buffering
          const filtered = newLogs.filter(log => !prev.some(p => p.event_id === log.event_id));
          return [...filtered, ...prev].slice(0, 100);
        });
      } catch (err) {
        console.error("Failed to parse SSE payload data:", err);
      }
    };
  };`
      },
      flattener: {
        title: "Nested Objects Recursive Flattener",
        desc: "Converts nested entity maps into flat key-value pairs (e.g. `entity.address.zip`) to simplify difference checks.",
        snippet: `// 2. Flatten nested maps recursively
function flattenObject(obj: any, prefix = ''): Record<string, any> {
  if (obj === null || obj === undefined) return {};
  if (typeof obj !== 'object') {
    return { [prefix]: obj };
  }
  let res: Record<string, any> = {};
  for (const key of Object.keys(obj)) {
    const val = obj[key];
    const newKey = prefix ? \`\${prefix}.\${key}\` : key;
    if (val !== null && typeof val === 'object' && !Array.isArray(val)) {
      Object.assign(res, flattenObject(val, newKey));
    } else {
      res[newKey] = val;
    }
  }
  return res;
}`
      },
      diff_calc: {
        title: "Side-by-Side Properties Delta Calculator",
        desc: "Compares flat keys of old vs new states, returning field status labels: Added, Deleted, Modified, or Unchanged.",
        snippet: `  // 3. Processing visual JSON differences
  const getDeltaFields = (log: CDCLog) => {
    const oldFlat = flattenObject(log.old_value || {});
    const newFlat = flattenObject(log.new_value || {});
    const allKeys = Array.from(new Set([...Object.keys(oldFlat), ...Object.keys(newFlat)])).sort();

    return allKeys.map(key => {
      const oldVal = oldFlat[key];
      const newVal = newFlat[key];
      const hasOld = key in oldFlat;
      const hasNew = key in newFlat;

      let status: 'unchanged' | 'modified' | 'added' | 'deleted' = 'unchanged';
      if (hasOld && !hasNew) {
        status = 'deleted';
      } else if (!hasOld && hasNew) {
        status = 'added';
      } else if (JSON.stringify(oldVal) !== JSON.stringify(newVal)) {
        status = 'modified';
      }

      return {
        key,
        oldValStr: hasOld ? JSON.stringify(oldVal, null, 2) : '-',
        newValStr: hasNew ? JSON.stringify(newVal, null, 2) : '-',
        status
      };
    });
  };`
      }
    }
  };

  return (
    <div className="fixed inset-0 z-50 overflow-y-auto bg-[#070b14]/98 backdrop-blur-xl flex flex-col font-sans text-slate-300 selection:bg-emerald-500/30 selection:text-emerald-400">
      
      {/* CSS layout tweaks for interactive flowchart */}
      <style dangerouslySetInnerHTML={{__html: `
        @keyframes flowDash {
          to {
            stroke-dashoffset: -40;
          }
        }
        .flow-path-active {
          stroke-dasharray: 8, 4;
          animation: flowDash 1.8s linear infinite;
        }
        .neon-shadow-active {
          box-shadow: 0 0 30px rgba(16, 185, 129, 0.25);
          border-color: rgba(16, 185, 129, 0.6) !important;
        }
        .log-terminal::-webkit-scrollbar {
          width: 6px;
        }
        .log-terminal::-webkit-scrollbar-thumb {
          background-color: rgba(255, 255, 255, 0.1);
          border-radius: 4px;
        }
        .no-scrollbar::-webkit-scrollbar {
          display: none;
        }
        .no-scrollbar {
          -ms-overflow-style: none;
          scrollbar-width: none;
        }
      `}} />

      {/* --------------------------------------------------------------------------
          DOCUMENTATION HEADER
          -------------------------------------------------------------------------- */}
      <header className="border-b border-slate-800/60 bg-[#0c1020]/90 backdrop-blur-md px-6 py-4 flex items-center justify-between sticky top-0 z-10">
        <div className="flex items-center space-x-3">
          <div className="flex items-center justify-center w-10 h-10 rounded-xl bg-emerald-500/10 text-emerald-400 border border-emerald-500/30 shadow-[0_0_15px_rgba(16,185,129,0.15)]">
            <BookOpen className="w-5 h-5" />
          </div>
          <div>
            <h1 className="text-xl font-bold text-white tracking-tight flex items-center">
              System Code Blueprint Explainer
              <span className="text-[10px] ml-2 px-1.5 py-0.5 rounded bg-emerald-500/15 text-emerald-400 border border-emerald-500/30 font-mono font-semibold uppercase">V2 PRO</span>
            </h1>
            <p className="text-xs text-slate-400">Detailed system mapping, live flowchart simulator, and block-wise code details</p>
          </div>
        </div>

        <button 
          onClick={onClose}
          className="p-1.5 rounded-lg bg-slate-800/50 hover:bg-slate-800 hover:text-white border border-slate-700/60 transition duration-200"
          title="Close Explainer Panel"
        >
          <X className="w-4 h-4" />
        </button>
      </header>

      {/* --------------------------------------------------------------------------
          NAVIGATION TAB BAR
          -------------------------------------------------------------------------- */}
      <div className="border-b border-slate-800/40 bg-[#070b14]/90 sticky top-[73px] z-10 px-6 pt-3 pb-3.5 flex space-x-2 overflow-x-auto overflow-y-hidden no-scrollbar">
        <button
          onClick={() => setActiveTab('architecture')}
          className={`flex items-center space-x-2 px-4 py-2 rounded-lg text-xs font-semibold font-mono uppercase tracking-wider transition border ${
            activeTab === 'architecture' 
              ? 'bg-emerald-500/10 border-emerald-500/30 text-emerald-400 shadow-[0_0_15px_rgba(16,185,129,0.15)]' 
              : 'border-transparent text-slate-400 hover:text-slate-200'
          }`}
        >
          <Activity className="w-4 h-4" />
          <span>Flowchart & Simulator</span>
        </button>

        <button
          onClick={() => setActiveTab('dataflow')}
          className={`flex items-center space-x-2 px-4 py-2 rounded-lg text-xs font-semibold font-mono uppercase tracking-wider transition border ${
            activeTab === 'dataflow' 
              ? 'bg-emerald-500/10 border-emerald-500/30 text-emerald-400 shadow-[0_0_15px_rgba(16,185,129,0.15)]' 
              : 'border-transparent text-slate-400 hover:text-slate-200'
          }`}
        >
          <GitMerge className="w-4 h-4" />
          <span>Data Transformation Flow</span>
        </button>

        <button
          onClick={() => setActiveTab('terraform')}
          className={`flex items-center space-x-2 px-4 py-2 rounded-lg text-xs font-semibold font-mono uppercase tracking-wider transition border ${
            activeTab === 'terraform' 
              ? 'bg-emerald-500/10 border-emerald-500/30 text-emerald-400 shadow-[0_0_15px_rgba(16,185,129,0.15)]' 
              : 'border-transparent text-slate-400 hover:text-slate-200'
          }`}
        >
          <Code2 className="w-4 h-4" />
          <span>Block 1: Terraform</span>
        </button>

        <button
          onClick={() => setActiveTab('ingestion')}
          className={`flex items-center space-x-2 px-4 py-2 rounded-lg text-xs font-semibold font-mono uppercase tracking-wider transition border ${
            activeTab === 'ingestion' 
              ? 'bg-emerald-500/10 border-emerald-500/30 text-emerald-400 shadow-[0_0_15px_rgba(16,185,129,0.15)]' 
              : 'border-transparent text-slate-400 hover:text-slate-200'
          }`}
        >
          <Cpu className="w-4 h-4" />
          <span>Block 2: Ingestion</span>
        </button>

        <button
          onClick={() => setActiveTab('middleware')}
          className={`flex items-center space-x-2 px-4 py-2 rounded-lg text-xs font-semibold font-mono uppercase tracking-wider transition border ${
            activeTab === 'middleware' 
              ? 'bg-emerald-500/10 border-emerald-500/30 text-emerald-400 shadow-[0_0_15px_rgba(16,185,129,0.15)]' 
              : 'border-transparent text-slate-400 hover:text-slate-200'
          }`}
        >
          <Server className="w-4 h-4" />
          <span>Block 3: Gateway</span>
        </button>

        <button
          onClick={() => setActiveTab('dashboard')}
          className={`flex items-center space-x-2 px-4 py-2 rounded-lg text-xs font-semibold font-mono uppercase tracking-wider transition border ${
            activeTab === 'dashboard' 
              ? 'bg-emerald-500/10 border-emerald-500/30 text-emerald-400 shadow-[0_0_15px_rgba(16,185,129,0.15)]' 
              : 'border-transparent text-slate-400 hover:text-slate-200'
          }`}
        >
          <Terminal className="w-4 h-4" />
          <span>Block 4: Dashboard</span>
        </button>
      </div>

      {/* --------------------------------------------------------------------------
          CONTENT PANEL
          -------------------------------------------------------------------------- */}
      <main className="flex-1 p-6 max-w-[1600px] w-full mx-auto space-y-6">

        {/* ==============================================================================
            TAB: ARCHITECTURE FLOWCHART & EVENT SIMULATOR
            ============================================================================== */}
        {activeTab === 'architecture' && (
          <div className="space-y-6 animate-fadeIn">
            
            {/* Flowchart canvas container */}
            <div className="bg-[#0b0e1a]/80 border border-slate-800/80 rounded-2xl p-6 relative overflow-hidden flow-glow">
              
              {/* Header inside Flowchart */}
              <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-4 mb-8">
                <div>
                  <span className="text-[10px] font-mono text-emerald-400 uppercase tracking-widest block font-bold">Visual Ingress Engine Flowchart</span>
                  <h2 className="text-lg font-bold text-white mt-0.5">Pipeline Processing Timeline</h2>
                </div>

                {/* Simulator controls */}
                <div className="flex items-center space-x-2">
                  <button
                    onClick={runSimulation}
                    disabled={isSimulating}
                    className={`flex items-center space-x-1.5 px-4 py-2 rounded-lg text-xs font-bold font-mono transition ${
                      isSimulating 
                        ? 'bg-slate-800 text-slate-500 cursor-not-allowed border border-slate-700/40' 
                        : 'bg-emerald-600 hover:bg-emerald-500 text-black shadow-lg shadow-emerald-500/10'
                    }`}
                  >
                    <Play className="w-3.5 h-3.5 fill-black" />
                    <span>{isSimulating ? 'SIMULATING...' : 'SIMULATE EVENT FLOW'}</span>
                  </button>

                  <button
                    onClick={resetSimulation}
                    className="p-2 rounded-lg bg-slate-800 hover:bg-slate-700 border border-slate-700/60 text-slate-300 hover:text-white transition"
                    title="Reset Simulator"
                  >
                    <RotateCcw className="w-3.5 h-3.5" />
                  </button>
                </div>
              </div>

              {/* Connected paths layout (SVG Overlay behind buttons) */}
              <div className="hidden lg:block absolute inset-x-0 top-1/2 -translate-y-16 h-8 pointer-events-none z-0">
                <svg className="w-full h-full" xmlns="http://www.w3.org/2000/svg">
                  {/* Background base path lines */}
                  <path d="M 80 16 L 1400 16" fill="none" stroke="rgba(255, 255, 255, 0.05)" strokeWidth="4" />
                  
                  {/* Dynamic event signal indicator paths */}
                  <path 
                    className={`flowing-path ${isSimulating ? 'flow-path-active' : ''}`} 
                    d="M 80 16 L 1400 16" 
                    fill="none" 
                    stroke={isSimulating ? '#10b981' : 'rgba(16, 185, 129, 0.2)'} 
                    strokeWidth="2.5" 
                  />
                </svg>
              </div>

              {/* Grid of pipeline stages */}
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-6 gap-6 relative z-10">
                {Object.entries(flowchartNodes).map(([key, value]) => {
                  const IconComponent = value.icon;
                  const isSelected = selectedNode === key;
                  const isActiveInSim = simStep === key;
                  
                  return (
                    <button
                      key={key}
                      onClick={() => {
                        if (!isSimulating) {
                          setSelectedNode(key);
                        }
                      }}
                      disabled={isSimulating}
                      className={`text-left p-4 rounded-xl border transition-all duration-300 flex flex-col justify-between h-48 bg-slate-900/60 relative ${
                        isActiveInSim 
                          ? 'neon-shadow-active bg-emerald-950/20' 
                          : isSelected 
                            ? 'border-emerald-500/50 bg-[#0d1326]/60 shadow-[0_0_20px_rgba(16,185,129,0.15)]' 
                            : 'border-slate-800/80 hover:border-slate-700/60'
                      }`}
                    >
                      <div className="flex items-start justify-between w-full">
                        <div className={`p-2.5 rounded-lg border ${
                          isActiveInSim
                            ? 'bg-emerald-500/20 border-emerald-400 text-emerald-300 shadow-[0_0_15px_rgba(16,185,129,0.2)]'
                            : isSelected 
                              ? 'bg-emerald-500/10 border-emerald-500/40 text-emerald-400' 
                              : 'bg-slate-800/60 border-slate-700/60 text-slate-400'
                        }`}>
                          <IconComponent className="w-5 h-5 animate-pulse-slow" />
                        </div>
                        
                        {/* Simulation step indicators */}
                        {isActiveInSim && (
                          <div className="flex items-center space-x-1">
                            <span className="text-[9px] font-mono font-bold text-emerald-400 bg-emerald-500/10 border border-emerald-500/20 px-1.5 py-0.5 rounded animate-pulse">ACTIVE</span>
                            <span className="w-2.5 h-2.5 rounded-full bg-emerald-400 animate-ping"></span>
                          </div>
                        )}
                      </div>

                      <div>
                        <h3 className={`text-xs font-mono font-bold uppercase tracking-wider mt-3 ${
                          isActiveInSim ? 'text-emerald-300' : isSelected ? 'text-white' : 'text-slate-300'
                        }`}>
                          {value.title.split('. ')[1]}
                        </h3>
                        <p className="text-[11px] text-slate-400 mt-1 line-clamp-3 leading-relaxed">
                          {value.desc}
                        </p>
                      </div>
                    </button>
                  );
                })}
              </div>

            </div>

            {/* Event Simulator Log Console (Rendered only when active/populated) */}
            {simLogs.length > 0 && (
              <div className="bg-[#05070f] border border-slate-900 rounded-xl p-4 flex flex-col font-mono text-xs">
                <div className="flex items-center space-x-2 border-b border-slate-900 pb-2 mb-3">
                  <Terminal className="w-4 h-4 text-emerald-400" />
                  <span className="text-slate-300 font-bold">Event Simulator Terminal Output</span>
                </div>
                <div className="space-y-2 max-h-40 overflow-y-auto log-terminal">
                  {simLogs.map((log, idx) => (
                    <div key={idx} className="flex items-center space-x-2 text-slate-400 animate-slideIn">
                      <span className="text-slate-600 text-[10px]">[{new Date().toLocaleTimeString()}]</span>
                      <span className="text-emerald-400">&gt;&gt;</span>
                      <span className="text-slate-300">{log}</span>
                    </div>
                  ))}
                  {isSimulating && (
                    <div className="flex items-center space-x-1.5 text-slate-500 italic animate-pulse">
                      <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-ping"></span>
                      <span>Listening for next pipeline sequence packet...</span>
                    </div>
                  )}
                </div>
              </div>
            )}

            {/* Explainer and Code walk for selected flowchart node */}
            {selectedNode && flowchartNodes[selectedNode] && (
              <div className="grid grid-cols-1 lg:grid-cols-5 gap-6 animate-slideIn">
                
                {/* Node details */}
                <div className="lg:col-span-2 bg-[#0c0f1e]/80 border border-slate-800/80 rounded-2xl p-6 space-y-6">
                  <div>
                    <span className="text-[10px] font-mono text-emerald-400 uppercase tracking-widest font-bold">Node Specifications</span>
                    <h2 className="text-xl font-bold text-white mt-0.5">{flowchartNodes[selectedNode].title}</h2>
                    <p className="text-xs text-slate-400 font-mono mt-0.5">{flowchartNodes[selectedNode].tech}</p>
                  </div>

                  <p className="text-sm text-slate-300 leading-relaxed bg-[#05070e] p-4 rounded-xl border border-slate-900">
                    {flowchartNodes[selectedNode].desc}
                  </p>

                  <div className="space-y-3.5">
                    <h4 className="text-xs font-bold font-mono text-slate-400 uppercase tracking-wider flex items-center gap-1.5">
                      <Info className="w-3.5 h-3.5 text-emerald-400" />
                      <span>Pipeline Integration Mechanics</span>
                    </h4>
                    <ul className="space-y-2.5">
                      {flowchartNodes[selectedNode].details.map((detail, idx) => (
                        <li key={idx} className="flex items-start text-xs text-slate-400 leading-relaxed">
                          <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 mr-2.5 mt-1.5"></span>
                          <span>{detail}</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                </div>

                {/* Node code handler snippet */}
                <div className="lg:col-span-3 bg-[#060810]/80 border border-slate-900 rounded-2xl flex flex-col overflow-hidden">
                  <div className="bg-[#0b0e19] border-b border-slate-900 px-4 py-3 flex items-center justify-between text-xs">
                    <div className="flex items-center space-x-2">
                      <Terminal className="w-4 h-4 text-emerald-400" />
                      <span className="font-mono text-slate-300 font-bold">Component Handler Script</span>
                    </div>
                    
                    <button
                      onClick={() => handleCopyCode(flowchartNodes[selectedNode].code, selectedNode)}
                      className="flex items-center space-x-1.5 px-2 py-1 rounded bg-slate-800 hover:bg-slate-700 text-slate-400 hover:text-white transition font-mono text-[10px]"
                    >
                      {copiedText === selectedNode ? <Check className="w-3 h-3 text-emerald-400" /> : <Copy className="w-3 h-3" />}
                      <span>{copiedText === selectedNode ? 'Copied!' : 'Copy'}</span>
                    </button>
                  </div>
                  <pre className="flex-1 p-5 overflow-auto font-mono text-[11px] leading-relaxed text-slate-300 bg-[#070b14]">
                    <code>{flowchartNodes[selectedNode].code}</code>
                  </pre>
                </div>

              </div>
            )}
          </div>
        )}
        {activeTab === 'dataflow' && (
          <div className="space-y-8 animate-fadeIn max-w-4xl mx-auto">
            <div className="text-center space-y-2 mb-8">
              <span className="text-[10px] font-mono text-emerald-400 uppercase tracking-widest font-bold">Trace Case-Study Walkthrough</span>
              <h2 className="text-2xl font-extrabold text-white">Live Data Transformation Lifecycle</h2>
              <p className="text-sm text-slate-400 font-mono">Behold the structural path of a Datastore property update of <span className="text-emerald-400 font-bold">Hardware/device-101</span> through the entire stack.</p>
            </div>

            {/* Steps Vertical Container */}
            <div className="relative border-l border-slate-800/80 ml-6 pl-8 space-y-12">
              
              {/* Step 1 */}
              <div className="relative">
                <div className="absolute -left-[45px] top-0 flex items-center justify-center w-8 h-8 rounded-full bg-emerald-500/10 border border-emerald-500/30 text-emerald-400 font-mono text-xs font-bold shadow-[0_0_10px_rgba(16,185,129,0.1)]">
                  1
                </div>
                <div className="bg-[#0b0e1a]/80 border border-slate-800/60 rounded-xl p-5 space-y-3">
                  <div className="flex items-center justify-between">
                    <h3 className="text-sm font-bold text-white flex items-center gap-2">
                      <Database className="w-4 h-4 text-emerald-400" />
                      <span>Original Database Write State (Datastore Entity)</span>
                    </h3>
                    <span className="text-[9px] font-mono text-slate-400 uppercase bg-slate-800 px-2 py-0.5 rounded">Source State</span>
                  </div>
                  <p className="text-xs text-slate-400 leading-relaxed">
                    A user updates the value field of a device from <span className="text-rose-400">3500</span> to <span className="text-emerald-400">3682</span>.
                  </p>
                  <pre className="p-3 bg-[#05070e] border border-slate-900 rounded-lg text-slate-300 text-[10px] font-mono overflow-auto">
{`Key: [Hardware/device-101]
Properties:
  - assetName: "Production Server"
  - value: 3682 (integer)
  - updatedAt: "2026-07-09T18:52:18Z"
  - updatedBy: "shreyashs14102002@gmail.com"
  - updatedByName: "Shreyash Sutane"`}
                  </pre>
                </div>
              </div>

              {/* Step 2 */}
              <div className="relative">
                <div className="absolute -left-[45px] top-0 flex items-center justify-center w-8 h-8 rounded-full bg-emerald-500/10 border border-emerald-500/30 text-emerald-400 font-mono text-xs font-bold shadow-[0_0_10px_rgba(16,185,129,0.1)]">
                  2
                </div>
                <div className="bg-[#0b0e1a]/80 border border-slate-800/60 rounded-xl p-5 space-y-3">
                  <div className="flex items-center justify-between">
                    <h3 className="text-sm font-bold text-white flex items-center gap-2">
                      <GitMerge className="w-4 h-4 text-emerald-400" />
                      <span>Eventarc Protobuf Envelope (CloudEvent Delivery Payload)</span>
                    </h3>
                    <span className="text-[9px] font-mono text-emerald-400 bg-emerald-500/10 px-2 py-0.5 rounded">Binary Proto Envelope</span>
                  </div>
                  <p className="text-xs text-slate-400 leading-relaxed">
                    Eventarc intercepts the write and delivers the event encoded in binary protobuf. Translating this envelope details the key paths and field wrappers:
                  </p>
                  <pre className="p-3 bg-[#05070e] border border-slate-900 rounded-lg text-slate-300 text-[10px] font-mono overflow-auto max-h-48">
{`{
  "value": {
    "entity": {
      "key": {
        "path": [
          { "kind": "Hardware", "name": "device-101", "id": null }
        ]
      },
      "properties": {
        "value": { "integer_value": "3682" },
        "assetName": { "string_value": "Production Server" },
        "updatedBy": { "string_value": "shreyashs14102002@gmail.com" },
        "updatedByName": { "string_value": "Shreyash Sutane" },
        "updatedAt": { "string_value": "2026-07-09T18:52:18Z" }
      }
    }
  },
  "old_value": {
    "entity": {
      "properties": {
        "value": { "integer_value": "3500" }
      }
    }
  }
}`}
                  </pre>
                </div>
              </div>

              {/* Step 3 */}
              <div className="relative">
                <div className="absolute -left-[45px] top-0 flex items-center justify-center w-8 h-8 rounded-full bg-emerald-500/10 border border-emerald-500/30 text-emerald-400 font-mono text-xs font-bold shadow-[0_0_10px_rgba(16,185,129,0.1)]">
                  3
                </div>
                <div className="bg-[#0b0e1a]/80 border border-slate-800/60 rounded-xl p-5 space-y-3">
                  <div className="flex items-center justify-between">
                    <h3 className="text-sm font-bold text-white flex items-center gap-2">
                      <Cpu className="w-4 h-4 text-emerald-400" />
                      <span>Ingested & Flattened BigQuery Row Ledger Format</span>
                    </h3>
                    <span className="text-[9px] font-mono text-cyan-400 bg-cyan-500/10 px-2 py-0.5 rounded">Structured Database Record</span>
                  </div>
                  <p className="text-xs text-slate-400 leading-relaxed">
                    The Python ingester unboxes the values (using recursion), maps key leaf node elements, extracts operator details, and creates a relational BigQuery insert:
                  </p>
                  <pre className="p-3 bg-[#05070e] border border-slate-900 rounded-lg text-slate-300 text-[10px] font-mono overflow-auto">
{`{
  "event_id": "event-9941a31-cdc",
  "execution_time": "2026-07-09T18:52:20.104Z",
  "operation_type": "UPDATE",
  "entity_kind": "Hardware",
  "entity_id": "device-101",
  "changed_by": "Shreyash Sutane (shreyashs14102002@gmail.com)",
  "old_value": "{\\"assetName\\": \\"Production Server\\", \\"value\\": 3500}",
  "new_value": "{\\"assetName\\": \\"Production Server\\", \\"value\\": 3682}"
}`}
                  </pre>
                </div>
              </div>

              {/* Step 4 */}
              <div className="relative">
                <div className="absolute -left-[45px] top-0 flex items-center justify-center w-8 h-8 rounded-full bg-emerald-500/10 border border-emerald-500/30 text-emerald-400 font-mono text-xs font-bold shadow-[0_0_10px_rgba(16,185,129,0.1)]">
                  4
                </div>
                <div className="bg-[#0b0e1a]/80 border border-slate-800/60 rounded-xl p-5 space-y-3">
                  <div className="flex items-center justify-between">
                    <h3 className="text-sm font-bold text-white flex items-center gap-2">
                      <Server className="w-4 h-4 text-emerald-400" />
                      <span>Persistent Server-Sent Events (SSE) Stream Payload</span>
                    </h3>
                    <span className="text-[9px] font-mono text-purple-400 bg-purple-500/10 px-2 py-0.5 rounded">SSE Chunk Output</span>
                  </div>
                  <p className="text-xs text-slate-400 leading-relaxed">
                    The Node.js Express server queries BigQuery and pushes the raw log arrays to all connected dashboard client listeners over an open HTTP pipeline:
                  </p>
                  <pre className="p-3 bg-[#05070e] border border-slate-900 rounded-lg text-slate-300 text-[10px] font-mono overflow-auto">
{`event: message
data: [{"event_id":"event-9941a31-cdc","execution_time":"2026-07-09T18:52:20.104Z","operation_type":"UPDATE","entity_kind":"Hardware","entity_id":"device-101","changed_by":"Shreyash Sutane (shreyashs14102002@gmail.com)","old_value":"{\\"assetName\\": \\"Production Server\\", \\"value\\": 3500}","new_value":"{\\"assetName\\": \\"Production Server\\", \\"value\\": 3682}"}]`}
                  </pre>
                </div>
              </div>

              {/* Step 5 */}
              <div className="relative">
                <div className="absolute -left-[45px] top-0 flex items-center justify-center w-8 h-8 rounded-full bg-emerald-500/10 border border-emerald-500/30 text-emerald-400 font-mono text-xs font-bold shadow-[0_0_10px_rgba(16,185,129,0.1)]">
                  5
                </div>
                <div className="bg-[#0b0e1a]/80 border border-slate-800/60 rounded-xl p-5 space-y-3">
                  <div className="flex items-center justify-between">
                    <h3 className="text-sm font-bold text-white flex items-center gap-2">
                      <Terminal className="w-4 h-4 text-emerald-400" />
                      <span>Client Dashboard Properties Delta Calculator Layout</span>
                    </h3>
                    <span className="text-[9px] font-mono text-pink-400 bg-pink-500/10 px-2 py-0.5 rounded">UI Render State</span>
                  </div>
                  <p className="text-xs text-slate-400 leading-relaxed">
                    The React client receives the SSE stream event, parses the JSON payload, compares key mappings recursively, and highlights differences:
                  </p>
                  
                  {/* Visual UI mimic card */}
                  <div className="bg-[#05070e] border border-slate-800/60 rounded-lg p-4 font-mono text-xs space-y-2">
                    <div className="flex items-center justify-between text-[10px] text-slate-400 border-b border-slate-900 pb-1.5 mb-2">
                      <span>PROPERTY FIELD</span>
                      <div className="flex space-x-8">
                        <span className="w-24 text-right">OLD VALUE</span>
                        <span className="w-24 text-right">NEW VALUE</span>
                        <span className="w-16 text-center">STATUS</span>
                      </div>
                    </div>
                    
                    <div className="flex items-center justify-between">
                      <span className="text-slate-300">assetName</span>
                      <div className="flex space-x-8 text-[11px]">
                        <span className="w-24 text-right text-slate-500">"Production Server"</span>
                        <span className="w-24 text-right text-slate-300">"Production Server"</span>
                        <span className="w-16 text-center text-slate-500 text-[10px]">UNCHANGED</span>
                      </div>
                    </div>

                    <div className="flex items-center justify-between bg-emerald-500/5 py-1 px-1.5 rounded border border-emerald-500/10">
                      <span className="text-emerald-400 font-bold">value</span>
                      <div className="flex space-x-8 text-[11px]">
                        <span className="w-24 text-right text-slate-400">3500</span>
                        <span className="w-24 text-right text-emerald-400 font-bold">3682</span>
                        <span className="w-16 text-center bg-emerald-500/15 text-emerald-400 text-[9px] font-bold rounded px-1">MODIFIED</span>
                      </div>
                    </div>
                  </div>
                </div>
              </div>

            </div>
          </div>
        )}

        {/* ==============================================================================
            TAB: DETAILED BLOCK WALKTHROUGHS (TERRAFORM, INGESTION, MIDDLEWARE, FRONTEND)
            ============================================================================== */}
        {activeTab !== 'architecture' && activeTab !== 'dataflow' && (
          <div className="grid grid-cols-1 lg:grid-cols-5 gap-6 animate-fadeIn">
            
            {/* Sidebar menu selection for block sections */}
            <div className="lg:col-span-2 space-y-6">
              
              {/* Intro Specs Card */}
              <div className="bg-[#0c0f1e]/80 border border-slate-800/80 rounded-2xl p-6 space-y-5">
                <div>
                  <span className="text-[10px] font-mono text-emerald-400 uppercase tracking-widest font-bold">
                    {activeTab === 'terraform' && 'Block 1 Blueprint'}
                    {activeTab === 'ingestion' && 'Block 2 Blueprint'}
                    {activeTab === 'middleware' && 'Block 3 Blueprint'}
                    {activeTab === 'dashboard' && 'Block 4 Blueprint'}
                  </span>
                  <h2 className="text-xl font-bold text-white mt-0.5">
                    {activeTab === 'terraform' && 'Infrastructure Provisioning'}
                    {activeTab === 'ingestion' && 'Data Ingestion Engine'}
                    {activeTab === 'middleware' && 'Audit Streaming Gateway'}
                    {activeTab === 'dashboard' && 'Visual Auditor Panel'}
                  </h2>
                  <p className="text-xs text-slate-400 font-mono mt-0.5">
                    {activeTab === 'terraform' && 'block1_terraform/main.tf'}
                    {activeTab === 'ingestion' && 'block2_ingestion/main.py'}
                    {activeTab === 'middleware' && 'block3_middleware/server.ts'}
                    {activeTab === 'dashboard' && 'block4_dashboard/src/App.tsx'}
                  </p>
                </div>

                <p className="text-xs text-slate-400 leading-relaxed bg-[#05070e] p-3 rounded-lg border border-slate-900">
                  Select a section below to highlight and explain specific modules of the codebase:
                </p>

                {/* Section selection list */}
                <div className="space-y-2">
                  {Object.entries(codeSections[activeTab]).map(([key, value]) => {
                    const isSelected = selectedSection === key;
                    return (
                      <button
                        key={key}
                        onClick={() => setSelectedSection(key)}
                        className={`w-full text-left p-3 rounded-xl border text-xs transition duration-150 flex items-center justify-between ${
                          isSelected 
                            ? 'bg-emerald-500/10 border-emerald-500/40 text-emerald-400 shadow-[0_0_15px_rgba(16,185,129,0.08)]' 
                            : 'bg-slate-900/40 border-slate-800/60 text-slate-400 hover:text-slate-200'
                        }`}
                      >
                        <div className="flex items-center space-x-2">
                          <FileCode className="w-4 h-4" />
                          <span className="font-bold">{value.title}</span>
                        </div>
                        <ArrowRight className={`w-3.5 h-3.5 transition-transform ${isSelected ? 'translate-x-1' : ''}`} />
                      </button>
                    );
                  })}
                </div>
              </div>

              {/* Selected Section Technical Details Card */}
              {selectedSection && codeSections[activeTab][selectedSection] && (
                <div className="bg-[#0c0f1e]/80 border border-slate-800/80 rounded-2xl p-6 space-y-4 animate-slideIn">
                  <div className="flex items-center space-x-2 border-b border-slate-800/60 pb-2">
                    <Sparkles className="w-4 h-4 text-emerald-400" />
                    <span className="font-mono text-[10px] text-slate-400 uppercase tracking-wider font-bold">Module Analysis</span>
                  </div>
                  <h3 className="text-sm font-bold text-white">{codeSections[activeTab][selectedSection].title}</h3>
                  <p className="text-xs text-slate-400 leading-relaxed">{codeSections[activeTab][selectedSection].desc}</p>
                </div>
              )}

            </div>

            {/* Right code walkthrough panel */}
            <div className="lg:col-span-3 bg-slate-950/80 border border-slate-900 rounded-2xl flex flex-col overflow-hidden">
              <div className="bg-[#0b0e19] border-b border-slate-900 px-4 py-3 flex items-center justify-between text-xs">
                <div className="flex items-center space-x-2">
                  <Terminal className="w-4 h-4 text-emerald-400" />
                  <span className="font-mono text-slate-300 font-bold">
                    {activeTab === 'terraform' && 'main.tf'}
                    {activeTab === 'ingestion' && 'main.py'}
                    {activeTab === 'middleware' && 'server.ts'}
                    {activeTab === 'dashboard' && 'App.tsx'}
                  </span>
                </div>
                
                <button
                  onClick={() => {
                    const codeText = selectedSection === 'all' 
                      ? defaultFullCode[activeTab] 
                      : codeSections[activeTab][selectedSection]?.snippet;
                    handleCopyCode(codeText, activeTab + selectedSection);
                  }}
                  className="flex items-center space-x-1.5 px-2 py-1 rounded bg-slate-800 hover:bg-slate-700 text-slate-400 hover:text-white transition font-mono text-[10px]"
                >
                  {copiedText === (activeTab + selectedSection) ? <Check className="w-3 h-3 text-emerald-400" /> : <Copy className="w-3 h-3" />}
                  <span>{copiedText === (activeTab + selectedSection) ? 'Copied!' : 'Copy Code'}</span>
                </button>
              </div>

              {/* Dynamically renders either full code file or the highlighted section snippet */}
              <pre className="flex-1 p-5 overflow-auto font-mono text-[11px] leading-relaxed text-slate-300 bg-[#070b14]">
                <code>
                  {selectedSection === 'all' 
                    ? defaultFullCode[activeTab] 
                    : codeSections[activeTab][selectedSection]?.snippet}
                </code>
              </pre>
            </div>

          </div>
        )}

      </main>
    </div>
  );
}

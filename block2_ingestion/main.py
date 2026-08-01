# ==============================================================================
# BLOCK 2: INGESTION PIPELINE (PYTHON CLOUD RUN FUNCTION)
# ==============================================================================

import os
import json
import logging
import datetime
import functions_framework
from google.cloud import bigquery

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("firestore-cdc-ingester")

# Initialize BigQuery Client (done globally for connection pooling)
try:
    bq_client = bigquery.Client()
    logger.info("Successfully initialized BigQuery client.")
except Exception as err:
    logger.error(f"Failed to initialize BigQuery client: {err}")
    bq_client = None


def parse_firestore_value(val):
    """
    Recursively parses Firestore / Datastore Proto-JSON structured values into standard Python
    types (handling both camelCase Firestore and snake_case / properties-based Datastore schemas).
    """
    if val is None:
        return None
    
    if not isinstance(val, dict):
        return val

    # Handle standard value types (Firestore and Datastore names)
    if "stringValue" in val:
        return val["stringValue"]
    elif "string_value" in val:
        return val["string_value"]
        
    elif "integerValue" in val:
        return int(val["integerValue"])
    elif "integer_value" in val:
        return int(val["integer_value"])
        
    elif "doubleValue" in val:
        return float(val["doubleValue"])
    elif "double_value" in val:
        return float(val["double_value"])
        
    elif "booleanValue" in val:
        return val["booleanValue"]
    elif "boolean_value" in val:
        return val["boolean_value"]
        
    elif "timestampValue" in val:
        return val["timestampValue"]
    elif "timestamp_value" in val:
        return val["timestamp_value"]
        
    elif "bytesValue" in val:
        return val["bytesValue"]
    elif "bytes_value" in val:
        return val["bytes_value"]
        
    elif "referenceValue" in val:
        return val["referenceValue"]
    elif "reference_value" in val:
        return val["reference_value"]
        
    elif "geoPointValue" in val:
        return val["geoPointValue"]
    elif "geo_point_value" in val:
        return val["geo_point_value"]
        
    elif "arrayValue" in val:
        arr_val = val["arrayValue"]
        values = arr_val.get("values", [])
        return [parse_firestore_value(v) for v in values]
    elif "array_value" in val:
        arr_val = val["array_value"]
        values = arr_val.get("values", [])
        return [parse_firestore_value(v) for v in values]
        
    elif "mapValue" in val:
        map_val = val["mapValue"]
        fields = map_val.get("fields", {})
        return {k: parse_firestore_value(v) for k, v in fields.items()}
    elif "entityValue" in val:
        entity_val = val["entityValue"]
        properties = entity_val.get("properties", {})
        return {k: parse_firestore_value(v) for k, v in properties.items()}
    elif "entity_value" in val:
        entity_val = val["entity_value"]
        properties = entity_val.get("properties", {})
        return {k: parse_firestore_value(v) for k, v in properties.items()}
        
    elif "nullValue" in val:
        return None
    elif "null_value" in val:
        return None

    # Handle document/entity envelope structures containing "fields" or "properties"
    if "fields" in val:
        return {k: parse_firestore_value(v) for k, v in val["fields"].items()}
    if "properties" in val:
        return {k: parse_firestore_value(v) for k, v in val["properties"].items()}

    # Fallback/Recursive mapping for standard dictionaries
    return {k: parse_firestore_value(v) for k, v in val.items()}


def extract_entity_info(doc_name):
    """
    Parses a Firestore / Datastore resource path name to extract Entity Kind and ID.
    Formats:
    - Firestore Native: projects/{project}/databases/{database}/documents/{Kind}/{ID}
    - Firestore Datastore: projects/{project}/databases/{database}/namespaces/{namespace}/kinds/{Kind}/entities/{ID}
    """
    if not doc_name:
        return "UnknownKind", "UnknownId"
    
    try:
        # Datastore Mode parsing
        if "/entities/" in doc_name:
            parts = doc_name.split("/entities/")
            entity_id = parts[1] if len(parts) > 1 else "UnknownId"
            
            if "/kinds/" in doc_name:
                kind_parts = doc_name.split("/kinds/")
                kind = kind_parts[1].split("/")[0] if len(kind_parts) > 1 else "UnknownKind"
                return kind, entity_id
            return "UnknownKind", entity_id

        # Native Mode fallback parsing
        parts = doc_name.split("/documents/")
        if len(parts) >= 2:
            path = parts[1]
            segments = path.split("/")
            if len(segments) >= 2:
                kind = segments[0]
                entity_id = segments[-1]
                return kind, entity_id
            return "UnknownKind", path
            
        return "UnknownKind", doc_name
    except Exception as err:
        logger.warning(f"Error parsing entity info from name '{doc_name}': {err}")
        return "UnknownKind", doc_name


@functions_framework.http
def firestore_cdc_ingester(request):
    """
    HTTP Cloud Function entry point that handles Eventarc CloudEvent triggers.
    Parses Firestore/Datastore DocumentEvent and logs changes to BigQuery.
    """
    global bq_client
    # 1. Parse CloudEvent headers and body
    headers = request.headers
    raw_data = request.data
    
    # Extract Event ID (defensively checking headers and payload)
    event_id = headers.get("ce-id") or headers.get("Ce-Id")
    event_type = headers.get("ce-type")
    
    content_type = headers.get("content-type", "")
    logger.info(f"Received request with Content-Type: {content_type}")
    
    # Extract payload data based on Content-Type
    if "json" in content_type.lower():
        body = request.get_json(silent=True) or {}
        if not event_id:
            event_id = body.get("id")
        if not event_type:
            event_type = body.get("type")
        event_data = body.get("data") if isinstance(body.get("data"), dict) else body
    else:
        # Assume application/protobuf or fallback binary format
        try:
            from google.events.cloud.datastore import EntityEventData
            event_data_pb = EntityEventData.deserialize(raw_data)
            event_data = EntityEventData.to_dict(event_data_pb)
            logger.info("Successfully decoded Datastore EntityEventData from binary protobuf.")
        except Exception as err:
            logger.warning(f"Failed to decode binary payload as Datastore EntityEventData: {err}. Trying fallback as JSON.")
            try:
                # Fallback to UTF-8 decoded JSON if it is actually text
                body = json.loads(raw_data.decode("utf-8"))
                event_data = body.get("data") if isinstance(body.get("data"), dict) else body
            except Exception as json_err:
                logger.error(f"Fallback JSON decoding also failed: {json_err}")
                return ("Invalid or undecodable payload", 400)

    if not event_id:
        logger.warning("Event received without unique ID (ce-id). Generating fallback UUID.")
        import uuid
        event_id = str(uuid.uuid4())

    logger.info(f"Processing Event ID: {event_id}, Type: {event_type}")

    if not event_data:
        logger.error(f"Event {event_id} contained empty body/payload.")
        return ("Payload is empty", 400)

    value = event_data.get("value")
    old_value_raw = event_data.get("old_value") or event_data.get("oldValue")

    if not value and not old_value_raw:
        logger.warning(f"Event {event_id} has neither 'value' nor 'old_value'. Nothing to ingest.")
        return ("No entity state found in payload", 200)

    # 3. Determine Operation Type & target document reference
    if value and not old_value_raw:
        operation_type = "INSERT"
        target_doc = value
    elif value and old_value_raw:
        operation_type = "UPDATE"
        target_doc = value
    elif old_value_raw and not value:
        operation_type = "DELETE"
        target_doc = old_value_raw
    else:
        operation_type = "UNKNOWN"
        target_doc = value or old_value_raw

    # 4. Extract Entity Kind and ID
    # Datastore Mode structure: key: { path: [ { kind: 'User', id: 12345, name: '' } ] }
    # Datastore Mode structure: entity: { key: { path: [ { kind: 'User', id: 12345, name: '' } ] } }
    entity_dict = target_doc.get("entity", {}) if "entity" in target_doc else target_doc
    key_dict = entity_dict.get("key", {})
    path_elements = key_dict.get("path", [])
    
    if path_elements:
        leaf = path_elements[-1]
        entity_kind = leaf.get("kind", "UnknownKind")
        entity_id = str(leaf.get("name") or leaf.get("id") or "UnknownId")
    else:
        doc_name = entity_dict.get("name", "")
        entity_kind, entity_id = extract_entity_info(doc_name)

    # 5. Parse old & new states into clean, readable JSON dictionaries
    old_value_clean = parse_firestore_value(old_value_raw) if old_value_raw else None
    new_value_clean = parse_firestore_value(value) if value else None

    # Strip envelopes if present to keep only the properties/fields
    if isinstance(old_value_clean, dict):
        if "fields" in old_value_clean:
            old_value_clean = old_value_clean.get("fields")
        elif "properties" in old_value_clean:
            old_value_clean = old_value_clean.get("properties")
            
    if isinstance(new_value_clean, dict):
        if "fields" in new_value_clean:
            new_value_clean = new_value_clean.get("fields")
        elif "properties" in new_value_clean:
            new_value_clean = new_value_clean.get("properties")

    # 6. Extract Identity Metadata
    # Supporting nested '_meta' (updated_by) or flat fields (updatedBy/updatedByName)
    def get_user_from_state(state):
        if not state or not isinstance(state, dict):
            return None
        # Try flat fields first
        email = state.get("updatedBy")
        name = state.get("updatedByName")
        if name and email:
            return f"{name} ({email})"
        if email:
            return email
        if name:
            return name
        # Fallback to nested _meta
        meta = state.get("_meta")
        if isinstance(meta, dict):
            m_email = meta.get("updated_by")
            m_name = meta.get("updatedByName") or meta.get("updated_by_name")
            if m_name and m_email:
                return f"{m_name} ({m_email})"
            return m_email or m_name
        return None

    changed_by = get_user_from_state(new_value_clean) or get_user_from_state(old_value_clean) or "unknown"


    # 7. Format log record for BigQuery
    ingress_time = datetime.datetime.utcnow().isoformat()
    
    row_to_insert = {
        "event_id": str(event_id),
        "execution_time": ingress_time,
        "operation_type": operation_type,
        "entity_kind": entity_kind,
        "entity_id": entity_id,
        "changed_by": changed_by,
        "old_value": json.dumps(old_value_clean) if old_value_clean else None,
        "new_value": json.dumps(new_value_clean) if new_value_clean else None
    }

    logger.info(
        f"CDC Log details: Op={operation_type}, Kind={entity_kind}, ID={entity_id}, By={changed_by}"
    )

    # 8. Write ledger log to BigQuery table
    if not bq_client:
        logger.error("BigQuery client was not initialized. Attempting re-initialization.")
        try:
            bq_client = bigquery.Client()
        except Exception as err:
            logger.error(f"Re-initialization failed: {err}")
            return ("Internal Error: BigQuery client unavailable", 500)

    # Resolve target table: project.cdc_logging.datastore_mutations_ledger
    project_id = os.environ.get("GCP_PROJECT") or bq_client.project
    table_id = f"{project_id}.cdc_logging.datastore_mutations_ledger"

    try:
        # insert_rows_json handles conversion of old_value/new_value dicts to JSON fields automatically
        errors = bq_client.insert_rows_json(table_id, [row_to_insert])
        if errors:
            logger.error(f"BigQuery insert failed with errors: {errors}")
            # Raise exception to trigger Eventarc retry if necessary, or return error
            raise RuntimeError(f"BigQuery insertion failure: {errors}")
        logger.info(f"Successfully logged event {event_id} to BigQuery.")
    except Exception as err:
        logger.error(f"Failed writing to BigQuery ledger table: {err}")
        # Return 500 status to allow Eventarc retry mechanism to kick in for transient errors
        return (f"Failed writing log to ledger: {err}", 500)

    return ("Event logged successfully", 200)

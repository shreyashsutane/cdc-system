export interface CDCLog {
  event_id: string;
  execution_time: string;
  operation_type: 'INSERT' | 'UPDATE' | 'DELETE' | 'UNKNOWN';
  entity_kind: string;
  entity_id: string;
  changed_by: string;
  old_value: Record<string, any> | null;
  new_value: Record<string, any> | null;
}

export type ConnectionStatus = 'connecting' | 'connected' | 'disconnected';

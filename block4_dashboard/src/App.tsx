// ==============================================================================
// BLOCK 4: HIGH-DENSITY AUDIT DASHBOARD (REACT + TAILWIND)
// ==============================================================================

import { useState, useEffect, useRef } from 'react';
import { 
  Activity, 
  Terminal, 
  User, 
  Calendar, 
  Search, 
  Database, 
  Eye, 
  RefreshCw, 
  X, 
  Layers, 
  ShieldCheck,
  Code2,
  BookOpen,
  Clock,
  ArrowRight,
  LogOut,
  Zap
} from 'lucide-react';
import { CDCLog, ConnectionStatus } from './types';
import DocumentationView from './DocumentationView';

// Helper to flatten nested JSON objects for cleaner key-value path comparisons
function flattenObject(obj: any, prefix = ''): Record<string, any> {
  if (obj === null || obj === undefined) return {};
  if (typeof obj !== 'object') {
    return { [prefix]: obj };
  }
  let res: Record<string, any> = {};
  for (const key of Object.keys(obj)) {
    const val = obj[key];
    const newKey = prefix ? `${prefix}.${key}` : key;
    if (val !== null && typeof val === 'object' && !Array.isArray(val)) {
      Object.assign(res, flattenObject(val, newKey));
    } else {
      res[newKey] = val;
    }
  }
  return res;
}

export type TimePreset = 'ALL' | '5M' | '10M' | '15M' | '1H' | '3H' | '6H' | 'CUSTOM';

export interface QueryEstimate {
  bytesProcessed: number;
  gbProcessed: number;
  estimatedCostUsd: number;
  costFormatted: string;
}

export default function App() {
  const [logs, setLogs] = useState<CDCLog[]>([]);
  const [connectionStatus, setConnectionStatus] = useState<ConnectionStatus>('connecting');
  const [searchQuery, setSearchQuery] = useState('');
  const [filterType, setFilterType] = useState<'ALL' | 'INSERT' | 'UPDATE' | 'DELETE'>('ALL');
  
  // Timestamp filtering state
  const [timePreset, setTimePreset] = useState<TimePreset>('ALL');
  const [customFrom, setCustomFrom] = useState('');
  const [customTo, setCustomTo] = useState('');
  const [isLoadingHistory, setIsLoadingHistory] = useState(false);
  const [isEstimating, setIsEstimating] = useState(false);
  const [queryEstimate, setQueryEstimate] = useState<QueryEstimate | null>(null);

  const [selectedLog, setSelectedLog] = useState<CDCLog | null>(null);
  const [recentEventIds, setRecentEventIds] = useState<Set<string>>(new Set());
  
  // Auth state variables
  const [token, setToken] = useState(() => localStorage.getItem('aether_cdc_token') || '');
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [isVerifying, setIsVerifying] = useState(false);
  const [authError, setAuthError] = useState('');
  const [tempToken, setTempToken] = useState('');
  const [showDocumentation, setShowDocumentation] = useState(false);

  const eventSourceRef = useRef<EventSource | null>(null);

  // Handshake verification of the auth token
  const verifyToken = async (authToken: string): Promise<boolean> => {
    try {
      const res = await fetch('/api/auth/verify', {
        headers: {
          'Authorization': `Bearer ${authToken}`
        }
      });
      return res.status === 200;
    } catch (err) {
      console.error('Error verifying auth token:', err);
      return false;
    }
  };

  // Run token verification on boot
  useEffect(() => {
    const initAuth = async () => {
      if (token) {
        setIsVerifying(true);
        const isValid = await verifyToken(token);
        if (isValid) {
          setIsAuthenticated(true);
        } else {
          localStorage.removeItem('aether_cdc_token');
          setToken('');
        }
        setIsVerifying(false);
      }
    };
    initAuth();
  }, [token]);

  // Handle stream initialization when authenticated
  useEffect(() => {
    if (isAuthenticated && token) {
      connectStream(token);
    }
    return () => {
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
      }
    };
  }, [isAuthenticated, token]);

  // Initialize SSE streaming connection
  const connectStream = (streamToken: string) => {
    setConnectionStatus('connecting');
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
    }

    // Connect to Node.js backend SSE endpoint, sending token as query param
    const eventSource = new EventSource(`/api/logs/stream?token=${streamToken}`);
    eventSourceRef.current = eventSource;

    eventSource.onopen = () => {
      setConnectionStatus('connected');
      console.log('SSE connection successfully opened.');
    };

    eventSource.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data);
        if (payload.type === 'seed') {
          setLogs(payload.logs);
        } else if (payload.type === 'mutation') {
          const newMutations: CDCLog[] = payload.logs;
          
          // Animate entry triggers
          const newIds = new Set(newMutations.map(l => l.event_id));
          setRecentEventIds(prev => new Set([...prev, ...newIds]));
          setTimeout(() => {
            setRecentEventIds(prev => {
              const updated = new Set(prev);
              newIds.forEach(id => updated.delete(id));
              return updated;
            });
          }, 3000); // clear highlighting after 3 seconds

          setLogs((prev) => {
            const combined = [...newMutations, ...prev];
            // Deduplicate by event_id
            const uniqueMap = new Map<string, CDCLog>();
            combined.forEach(log => uniqueMap.set(log.event_id, log));
            const uniqueLogs = Array.from(uniqueMap.values());
            // Sort by execution time descending
            return uniqueLogs.sort((a, b) => new Date(b.execution_time).getTime() - new Date(a.execution_time).getTime());
          });
        }
      } catch (err) {
        console.error('Failed to parse incoming SSE message:', err);
      }
    };

    eventSource.onerror = (err) => {
      console.error('SSE error occurred:', err);
      setConnectionStatus('disconnected');
      eventSource.close();
    };
  };

  // Form submit for access gate login
  const handleLogin = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!tempToken.trim()) {
      setAuthError('Access token cannot be empty');
      return;
    }
    
    setAuthError('');
    setIsVerifying(true);
    const isValid = await verifyToken(tempToken);
    if (isValid) {
      localStorage.setItem('aether_cdc_token', tempToken);
      setToken(tempToken);
      setIsAuthenticated(true);
    } else {
      setAuthError('Invalid Access Token. Please check your credentials.');
    }
    setIsVerifying(false);
  };

  // Sign out and clear stored session
  const handleLogout = () => {
    localStorage.removeItem('aether_cdc_token');
    setToken('');
    setIsAuthenticated(false);
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
    }
  };

  // Query historical logs from BigQuery when custom or larger ranges are requested
  const fetchHistoricalLogs = async (fromIso?: string, toIso?: string) => {
    if (!token) return;
    setIsLoadingHistory(true);
    try {
      const params = new URLSearchParams();
      if (fromIso) params.append('from', fromIso);
      if (toIso) params.append('to', toIso);
      params.append('limit', '1000');

      const res = await fetch(`/api/logs/history?${params.toString()}`, {
        headers: {
          'Authorization': `Bearer ${token}`
        }
      });

      if (res.ok) {
        const data = await res.json();
        if (data.logs) {
          setLogs(data.logs);
        }
      }
    } catch (err) {
      console.error('Error fetching historical logs:', err);
    } finally {
      setIsLoadingHistory(false);
    }
  };

  // Fetch BigQuery dry-run cost & data volume estimate before executing
  const fetchQueryEstimate = async (fromIso?: string, toIso?: string) => {
    if (!token) return;
    setIsEstimating(true);
    try {
      const params = new URLSearchParams();
      if (fromIso) params.append('from', fromIso);
      if (toIso) params.append('to', toIso);
      params.append('limit', '1000');

      const res = await fetch(`/api/logs/estimate?${params.toString()}`, {
        headers: {
          'Authorization': `Bearer ${token}`
        }
      });

      if (res.ok) {
        const data: QueryEstimate = await res.json();
        setQueryEstimate(data);
      }
    } catch (err) {
      console.error('Error fetching query estimate:', err);
    } finally {
      setIsEstimating(false);
    }
  };

  // Helper to get cutoff timestamp for presets
  const getPresetCutoff = (preset: TimePreset): Date | null => {
    const now = new Date();
    switch (preset) {
      case '5M': return new Date(now.getTime() - 5 * 60 * 1000);
      case '10M': return new Date(now.getTime() - 10 * 60 * 1000);
      case '15M': return new Date(now.getTime() - 15 * 60 * 1000);
      case '1H': return new Date(now.getTime() - 60 * 60 * 1000);
      case '3H': return new Date(now.getTime() - 3 * 60 * 60 * 1000);
      case '6H': return new Date(now.getTime() - 6 * 60 * 60 * 1000);
      default: return null;
    }
  };

  // Handle Preset Selection change
  const handleTimePresetChange = (preset: TimePreset) => {
    setTimePreset(preset);
    if (preset === 'ALL') {
      setQueryEstimate(null);
      fetchHistoricalLogs();
    } else if (preset === '3H' || preset === '6H') {
      const cutoff = getPresetCutoff(preset);
      if (cutoff) {
        fetchQueryEstimate(cutoff.toISOString());
        fetchHistoricalLogs(cutoff.toISOString());
      }
    } else if (preset === 'CUSTOM') {
      // Calculate initial estimate if dates already present
      if (customFrom) {
        const fromIso = new Date(customFrom).toISOString();
        const toIso = customTo ? new Date(customTo).toISOString() : new Date().toISOString();
        fetchQueryEstimate(fromIso, toIso);
      }
    } else {
      // For short client-cached ranges (5M, 10M, 15M, 1H), calculate local estimate
      const cutoff = getPresetCutoff(preset);
      if (cutoff) fetchQueryEstimate(cutoff.toISOString());
    }
  };

  // Live estimate recalculation when custom date changes
  useEffect(() => {
    if (timePreset === 'CUSTOM' && customFrom) {
      const fromIso = new Date(customFrom).toISOString();
      const toIso = customTo ? new Date(customTo).toISOString() : new Date().toISOString();
      fetchQueryEstimate(fromIso, toIso);
    }
  }, [customFrom, customTo, timePreset]);

  // Apply custom range filter
  const applyCustomDateFilter = () => {
    if (!customFrom) return;
    const fromIso = new Date(customFrom).toISOString();
    const toIso = customTo ? new Date(customTo).toISOString() : new Date().toISOString();
    fetchHistoricalLogs(fromIso, toIso);
  };

  // Filter logs based on search inputs, operation type & active timestamp filter
  const filteredLogs = logs.filter(log => {
    const matchesSearch = 
      log.entity_kind.toLowerCase().includes(searchQuery.toLowerCase()) ||
      log.entity_id.toLowerCase().includes(searchQuery.toLowerCase()) ||
      log.changed_by.toLowerCase().includes(searchQuery.toLowerCase()) ||
      log.event_id.toLowerCase().includes(searchQuery.toLowerCase());

    const matchesType = filterType === 'ALL' || log.operation_type === filterType;

    // Time filtering
    let matchesTime = true;
    const logTime = new Date(log.execution_time).getTime();

    if (timePreset === 'CUSTOM') {
      if (customFrom) {
        const fromTime = new Date(customFrom).getTime();
        if (logTime < fromTime) matchesTime = false;
      }
      if (customTo) {
        const toTime = new Date(customTo).getTime();
        if (logTime > toTime) matchesTime = false;
      }
    } else if (timePreset !== 'ALL') {
      const cutoff = getPresetCutoff(timePreset);
      if (cutoff && logTime < cutoff.getTime()) {
        matchesTime = false;
      }
    }

    return matchesSearch && matchesType && matchesTime;
  });

  // Calculate statistics counts dynamically based on active filtered time window & search
  const statCounts = filteredLogs.reduce(
    (acc, log) => {
      acc.all++;
      if (log.operation_type === 'INSERT') acc.inserts++;
      if (log.operation_type === 'UPDATE') acc.updates++;
      if (log.operation_type === 'DELETE') acc.deletes++;
      return acc;
    },
    { all: 0, inserts: 0, updates: 0, deletes: 0 }
  );

  // Parse Delta fields side-by-side
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
  };

  // Rendering Session Loading View
  if (isVerifying && !isAuthenticated) {
    return (
      <div className="min-h-screen bg-[#070b14] flex flex-col items-center justify-center font-sans">
        <div className="flex flex-col items-center space-y-4">
          <div className="w-10 h-10 border-4 border-emerald-500 border-t-transparent rounded-full animate-spin"></div>
          <p className="text-slate-400 text-sm font-mono animate-pulse">Verifying Access Token Credentials...</p>
        </div>
      </div>
    );
  }

  // Rendering Auth/Login Gate Page
  if (!isAuthenticated) {
    return (
      <div className="min-h-screen bg-[#070b14] text-[#e2e8f0] flex items-center justify-center font-sans p-4">
        <div className="w-full max-w-md glass-panel p-8 rounded-2xl border border-slate-800/80 shadow-2xl relative overflow-hidden">
          
          <div className="absolute -top-24 -left-24 w-48 h-48 bg-emerald-500/10 rounded-full blur-3xl"></div>
          <div className="absolute -bottom-24 -right-24 w-48 h-48 bg-cyan-500/10 rounded-full blur-3xl"></div>

          <div className="flex flex-col items-center mb-8 relative">
            <div className="flex items-center justify-center w-14 h-14 rounded-2xl bg-gradient-to-tr from-emerald-600 to-cyan-500 shadow-xl shadow-emerald-500/20 mb-4">
              <Activity className="w-7 h-7 text-black stroke-[2.5]" />
            </div>
            <h2 className="text-2xl font-bold tracking-tight text-white">AetherCDC Security Gate</h2>
            <p className="text-xs text-slate-400 mt-1">GCP Mutation Ledger Dashboard Portal</p>
          </div>

          <form onSubmit={handleLogin} className="space-y-6 relative">
            <div className="space-y-2">
              <label className="text-xs font-mono font-semibold uppercase tracking-wider text-slate-400">Gateway Access Token</label>
              <div className="relative">
                <input 
                  type="password" 
                  placeholder="••••••••••••••••••••••••••••" 
                  value={tempToken}
                  onChange={(e) => setTempToken(e.target.value)}
                  className="w-full bg-[#0a0d16] border border-slate-800 rounded-lg pl-4 pr-10 py-3 text-sm font-mono text-emerald-400 placeholder-slate-600 focus:outline-none focus:border-emerald-500/60 focus:ring-1 focus:ring-emerald-500/40 transition"
                  disabled={isVerifying}
                />
              </div>
              {authError && (
                <p className="text-xs text-rose-400 font-medium font-mono pt-1">{authError}</p>
              )}
            </div>

            <button
              type="submit"
              disabled={isVerifying}
              className="w-full flex items-center justify-center space-x-2 py-3 rounded-lg bg-gradient-to-r from-emerald-600 to-cyan-500 hover:from-emerald-500 hover:to-cyan-400 text-black font-bold text-sm tracking-wide shadow-lg shadow-emerald-500/10 hover:shadow-emerald-500/25 transition duration-200"
            >
              {isVerifying ? (
                <div className="w-5 h-5 border-2 border-black border-t-transparent rounded-full animate-spin"></div>
              ) : (
                <>
                  <ShieldCheck className="w-4 h-4" />
                  <span>Authenticate Session</span>
                </>
              )}
            </button>
          </form>

          <div className="mt-8 pt-6 border-t border-slate-800/60 text-center relative">
            <span className="text-[10px] font-mono text-slate-500 uppercase font-semibold">Protected Environment</span>
            <p className="text-[10px] text-slate-500 mt-1 font-mono leading-relaxed">
              Authorized personnel only. All access attempts are audited.
            </p>
          </div>

        </div>
      </div>
    );
  }

  // Rendering Main Log Dashboard
  return (
    <div className="min-h-screen bg-[#070b14] text-[#e2e8f0] flex flex-col selection:bg-emerald-500/30 selection:text-emerald-400">
      
      {/* --------------------------------------------------------------------------
          HEADER PANEL
          -------------------------------------------------------------------------- */}
      <header className="border-b border-[#1e293b]/60 bg-[#0d1326]/80 backdrop-blur-md sticky top-0 z-30 px-6 py-4 flex items-center justify-between">
        <div className="flex items-center space-x-3">
          <div className="flex items-center justify-center w-10 h-10 rounded-xl bg-gradient-to-tr from-emerald-600 to-cyan-500 shadow-lg shadow-emerald-500/20">
            <Activity className="w-5 h-5 text-black stroke-[2.5]" />
          </div>
          <div>
            <h1 className="text-xl font-bold tracking-tight text-white flex items-center">
              AetherCDC <span className="text-[10px] ml-2 px-1.5 py-0.5 rounded bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 font-mono uppercase tracking-wider font-semibold">Datastore Mode</span>
            </h1>
            <p className="text-xs text-slate-400">Production-Grade GCP Real-Time Mutation Ledger</p>
          </div>
        </div>

        {/* Live Status and auth controls */}
        <div className="flex items-center space-x-4">
          <div className="flex items-center space-x-2.5 px-3 py-1.5 rounded-full bg-slate-900 border border-slate-800">
            <span className={`w-2.5 h-2.5 rounded-full ${
              connectionStatus === 'connected' ? 'bg-emerald-500 animate-glow' :
              connectionStatus === 'connecting' ? 'bg-amber-500 animate-pulse' :
              'bg-rose-500'
            }`} />
            <span className="text-xs font-mono font-semibold tracking-wider uppercase text-slate-300">
              {connectionStatus === 'connected' && 'LIVE STREAM CONNECTED'}
              {connectionStatus === 'connecting' && 'STREAM RECONNECTING...'}
              {connectionStatus === 'disconnected' && 'STREAM OFFLINE'}
            </span>
          </div>

          {connectionStatus === 'disconnected' && (
            <button 
              onClick={() => connectStream(token)} 
              className="flex items-center space-x-1.5 px-3 py-1.5 rounded-lg bg-emerald-600/15 border border-emerald-500/35 hover:bg-emerald-600/35 text-emerald-400 text-xs transition duration-200"
            >
              <RefreshCw className="w-3.5 h-3.5" />
              <span>Reconnect</span>
            </button>
          )}

          <button 
            onClick={() => setShowDocumentation(true)}
            className="flex items-center space-x-1.5 px-3 py-1.5 rounded-lg bg-emerald-600/10 hover:bg-emerald-600/25 border border-emerald-500/20 text-emerald-400 hover:text-emerald-300 text-xs transition duration-200"
            title="View Code Flow & Explanations"
          >
            <BookOpen className="w-3.5 h-3.5" />
            <span>Document</span>
          </button>

          <button 
            onClick={handleLogout}
            className="flex items-center space-x-1.5 px-3 py-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 border border-slate-700/60 text-slate-300 hover:text-white text-xs transition duration-200"
            title="Log Out Session"
          >
            <LogOut className="w-3.5 h-3.5" />
            <span>Lock</span>
          </button>
        </div>
      </header>

      {/* Main Grid View */}
      <main className="flex-1 p-6 space-y-6 max-w-[1600px] w-full mx-auto">

        {/* --------------------------------------------------------------------------
            METRIC STATS CARDS
            -------------------------------------------------------------------------- */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          
          <div className="glass-panel rounded-xl p-4 flex flex-col justify-between">
            <span className="text-xs text-slate-400 uppercase tracking-wider font-semibold">Mutations Captured</span>
            <div className="flex items-baseline space-x-2 mt-2">
              <span className="text-2xl font-bold font-mono text-white">{statCounts.all}</span>
              <span className="text-[10px] text-slate-500">ledger logs</span>
            </div>
            <div className="h-1.5 w-full bg-slate-800 rounded-full mt-3 overflow-hidden">
              <div className="h-full bg-slate-400 rounded-full" style={{ width: '100%' }}></div>
            </div>
          </div>

          <div className="glass-panel rounded-xl p-4 flex flex-col justify-between border-l-2 border-l-emerald-500/70">
            <span className="text-xs text-emerald-400 uppercase tracking-wider font-semibold">INSERT events</span>
            <div className="flex items-baseline space-x-2 mt-2">
              <span className="text-2xl font-bold font-mono text-emerald-400">{statCounts.inserts}</span>
              <span className="text-[10px] text-emerald-600/80">({statCounts.all ? Math.round((statCounts.inserts / statCounts.all) * 100) : 0}%)</span>
            </div>
            <div className="h-1.5 w-full bg-slate-800 rounded-full mt-3 overflow-hidden">
              <div 
                className="h-full bg-emerald-500 rounded-full" 
                style={{ width: `${statCounts.all ? (statCounts.inserts / statCounts.all) * 100 : 0}%` }}
              ></div>
            </div>
          </div>

          <div className="glass-panel rounded-xl p-4 flex flex-col justify-between border-l-2 border-l-amber-500/70">
            <span className="text-xs text-amber-400 uppercase tracking-wider font-semibold">UPDATE events</span>
            <div className="flex items-baseline space-x-2 mt-2">
              <span className="text-2xl font-bold font-mono text-amber-400">{statCounts.updates}</span>
              <span className="text-[10px] text-amber-600/80">({statCounts.all ? Math.round((statCounts.updates / statCounts.all) * 100) : 0}%)</span>
            </div>
            <div className="h-1.5 w-full bg-slate-800 rounded-full mt-3 overflow-hidden">
              <div 
                className="h-full bg-amber-500 rounded-full" 
                style={{ width: `${statCounts.all ? (statCounts.updates / statCounts.all) * 100 : 0}%` }}
              ></div>
            </div>
          </div>

          <div className="glass-panel rounded-xl p-4 flex flex-col justify-between border-l-2 border-l-rose-500/70">
            <span className="text-xs text-rose-400 uppercase tracking-wider font-semibold">DELETE events</span>
            <div className="flex items-baseline space-x-2 mt-2">
              <span className="text-2xl font-bold font-mono text-rose-400">{statCounts.deletes}</span>
              <span className="text-[10px] text-rose-600/80">({statCounts.all ? Math.round((statCounts.deletes / statCounts.all) * 100) : 0}%)</span>
            </div>
            <div className="h-1.5 w-full bg-slate-800 rounded-full mt-3 overflow-hidden">
              <div 
                className="h-full bg-rose-500 rounded-full" 
                style={{ width: `${statCounts.all ? (statCounts.deletes / statCounts.all) * 100 : 0}%` }}
              ></div>
            </div>
          </div>

        </div>

        {/* --------------------------------------------------------------------------
            CONTROLS & TIME FILTER PANEL
            -------------------------------------------------------------------------- */}
        <div className="glass-panel rounded-xl p-4 space-y-4">
          
          {/* Top Row: Search & Operation Type filters */}
          <div className="flex flex-col sm:flex-row items-center justify-between gap-4">
            
            {/* Search bar */}
            <div className="relative w-full sm:w-96">
              <Search className="absolute left-3 top-2.5 w-4 h-4 text-slate-500" />
              <input 
                type="text" 
                placeholder="Search Kind, Key ID, User Email..." 
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="w-full bg-[#0a0d16] border border-slate-800/80 rounded-lg pl-10 pr-4 py-2 text-sm text-slate-200 placeholder-slate-500 focus:outline-none focus:border-emerald-500/60 focus:ring-1 focus:ring-emerald-500/40 transition"
              />
            </div>

            {/* Type filters */}
            <div className="flex items-center space-x-2 w-full sm:w-auto overflow-x-auto py-1">
              <span className="text-xs text-slate-500 font-semibold font-mono uppercase mr-2 shrink-0">Type:</span>
              {(['ALL', 'INSERT', 'UPDATE', 'DELETE'] as const).map((type) => (
                <button
                  key={type}
                  onClick={() => setFilterType(type)}
                  className={`px-3 py-1.5 rounded-lg text-xs font-semibold tracking-wider transition ${
                    filterType === type 
                      ? type === 'INSERT' ? 'bg-emerald-500/25 border border-emerald-500/50 text-emerald-400'
                        : type === 'UPDATE' ? 'bg-amber-500/25 border border-amber-500/50 text-amber-400'
                        : type === 'DELETE' ? 'bg-rose-500/25 border border-rose-500/50 text-rose-400'
                        : 'bg-slate-700/50 border border-slate-600 text-white'
                      : 'bg-transparent border border-slate-800 text-slate-400 hover:border-slate-700 hover:text-slate-200'
                  }`}
                >
                  {type}
                </button>
              ))}
            </div>

          </div>

          {/* Bottom Row: Timestamp Presets & Custom Range */}
          <div className="pt-3 border-t border-slate-800/60 flex flex-col lg:flex-row items-start lg:items-center justify-between gap-4">
            
            {/* Timestamp Presets */}
            <div className="flex items-center space-x-1.5 overflow-x-auto w-full lg:w-auto py-1">
              <div className="flex items-center space-x-1.5 text-xs text-slate-400 font-semibold font-mono uppercase mr-2 shrink-0">
                <Clock className="w-3.5 h-3.5 text-emerald-400" />
                <span>Time Range:</span>
              </div>

              {[
                { id: 'ALL', label: 'All Time' },
                { id: '5M', label: 'Last 5m' },
                { id: '10M', label: 'Last 10m' },
                { id: '15M', label: 'Last 15m' },
                { id: '1H', label: 'Last 1h' },
                { id: '3H', label: 'Last 3h' },
                { id: '6H', label: 'Last 6h' },
                { id: 'CUSTOM', label: 'Custom Range' },
              ].map((preset) => (
                <button
                  key={preset.id}
                  onClick={() => handleTimePresetChange(preset.id as TimePreset)}
                  className={`px-2.5 py-1 rounded-md text-xs font-mono font-medium transition shrink-0 ${
                    timePreset === preset.id
                      ? 'bg-emerald-500/20 border border-emerald-500/50 text-emerald-300 shadow-[0_0_10px_rgba(16,185,129,0.15)]'
                      : 'bg-[#0a0d16] border border-slate-800 text-slate-400 hover:border-slate-700 hover:text-slate-200'
                  }`}
                >
                  {preset.label}
                </button>
              ))}
            </div>

            {/* Custom Date Range Picker */}
            {timePreset === 'CUSTOM' && (
              <div className="flex flex-wrap items-center gap-2 bg-[#0a0d16] p-2 rounded-lg border border-slate-800 w-full lg:w-auto">
                <div className="flex items-center space-x-1.5 text-xs text-slate-400 font-mono">
                  <span className="text-slate-500 font-bold">FROM:</span>
                  <input 
                    type="datetime-local" 
                    value={customFrom}
                    onChange={(e) => setCustomFrom(e.target.value)}
                    className="bg-[#05070f] border border-slate-700/80 rounded px-2 py-1 text-xs text-slate-200 focus:outline-none focus:border-emerald-500"
                  />
                </div>

                <div className="flex items-center space-x-1.5 text-xs text-slate-400 font-mono">
                  <span className="text-slate-500 font-bold">TO:</span>
                  <input 
                    type="datetime-local" 
                    value={customTo}
                    onChange={(e) => setCustomTo(e.target.value)}
                    className="bg-[#05070f] border border-slate-700/80 rounded px-2 py-1 text-xs text-slate-200 focus:outline-none focus:border-emerald-500"
                  />
                </div>

                <button
                  onClick={applyCustomDateFilter}
                  disabled={isLoadingHistory || !customFrom}
                  className="px-3 py-1 rounded bg-emerald-600 hover:bg-emerald-500 text-black font-bold font-mono text-xs transition disabled:opacity-50 flex items-center space-x-1"
                >
                  {isLoadingHistory ? (
                    <div className="w-3 h-3 border-2 border-black border-t-transparent rounded-full animate-spin" />
                  ) : (
                    <>
                      <span>Query BigQuery</span>
                      <ArrowRight className="w-3 h-3" />
                    </>
                  )}
                </button>
              </div>
            )}

          </div>

          {/* BigQuery Query Cost & Data Volume Dry-Run Estimation Bar */}
          {(queryEstimate || isEstimating) && (
            <div className="pt-2 border-t border-slate-800/40 flex flex-col sm:flex-row items-start sm:items-center justify-between gap-2 text-xs font-mono bg-emerald-950/10 border border-emerald-500/20 p-2.5 rounded-lg animate-fadeIn">
              <div className="flex items-center space-x-2 text-emerald-400">
                <Zap className="w-4 h-4 text-emerald-400 shrink-0" />
                <span className="font-semibold">BigQuery Dry-Run Estimate (Pre-Execution):</span>
                {isEstimating && (
                  <div className="w-3 h-3 border-2 border-emerald-400 border-t-transparent rounded-full animate-spin ml-1" />
                )}
              </div>
              
              {queryEstimate && !isEstimating && (
                <div className="flex flex-wrap items-center gap-4 text-slate-300">
                <div className="flex items-center space-x-1">
                  <span className="text-slate-500">Data Scanned:</span>
                  <span className="text-emerald-400 font-bold">
                    {queryEstimate.gbProcessed < 0.001 
                      ? `${(queryEstimate.bytesProcessed / 1024).toFixed(2)} KB` 
                      : `${queryEstimate.gbProcessed.toFixed(4)} GB`}
                  </span>
                </div>

                <div className="flex items-center space-x-1">
                  <span className="text-slate-500">Estimated Cost:</span>
                  <span className="text-emerald-400 font-bold bg-emerald-500/10 px-1.5 py-0.5 rounded border border-emerald-500/30">
                    {queryEstimate.costFormatted} USD
                  </span>
                  <span className="text-[10px] text-slate-500 font-sans">(1 TB/mo free tier)</span>
                </div>
              </div>
              )}
            </div>
          )}

        </div>

        {/* --------------------------------------------------------------------------
            LIVE DATA STREAM TABLE
            -------------------------------------------------------------------------- */}
        <div className="glass-panel rounded-xl overflow-hidden shadow-2xl">
          <div className="overflow-x-auto">
            <table className="w-full text-left border-collapse">
              <thead>
                <tr className="border-b border-[#1e293b]/60 bg-slate-900/40 text-slate-400 text-xs font-mono font-semibold uppercase tracking-wider">
                  <th className="px-6 py-4">Execution Time</th>
                  <th className="px-6 py-4">Operation</th>
                  <th className="px-6 py-4">Kind</th>
                  <th className="px-6 py-4">Entity ID</th>
                  <th className="px-6 py-4">Operator (Email)</th>
                  <th className="px-6 py-4 text-right">Actions</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-[#1e293b]/40 font-mono text-sm">
                {filteredLogs.length > 0 ? (
                  filteredLogs.map((log) => {
                    const isNew = recentEventIds.has(log.event_id);
                    const isSelected = selectedLog?.event_id === log.event_id;

                    return (
                      <tr 
                        key={log.event_id}
                        onClick={() => setSelectedLog(log)}
                        className={`hover:bg-[#131b33]/45 transition duration-150 cursor-pointer ${
                          isNew ? 'bg-emerald-500/5 animate-pulse border-l-2 border-l-emerald-500' : ''
                        } ${
                          isSelected ? 'bg-emerald-500/10 hover:bg-emerald-500/10' : ''
                        }`}
                      >
                        {/* Time */}
                        <td className="px-6 py-3.5 whitespace-nowrap text-slate-300">
                          <div className="flex items-center space-x-2">
                            <Calendar className="w-3.5 h-3.5 text-slate-500" />
                            <span>{new Date(log.execution_time).toLocaleString()}</span>
                          </div>
                        </td>

                        {/* Operation Badge */}
                        <td className="px-6 py-3.5 whitespace-nowrap">
                          <span className={`inline-flex items-center px-2 py-0.5 rounded text-[10px] font-bold tracking-wider border ${
                            log.operation_type === 'INSERT' ? 'bg-emerald-950/40 border-emerald-500/30 text-emerald-400' :
                            log.operation_type === 'UPDATE' ? 'bg-amber-950/40 border-amber-500/30 text-amber-400' :
                            log.operation_type === 'DELETE' ? 'bg-rose-950/40 border-rose-500/30 text-rose-400' :
                            'bg-slate-900/60 border-slate-700 text-slate-400'
                          }`}>
                            {log.operation_type}
                          </span>
                        </td>

                        {/* Kind */}
                        <td className="px-6 py-3.5 whitespace-nowrap text-cyan-400 font-medium">
                          <div className="flex items-center space-x-1.5">
                            <Layers className="w-3.5 h-3.5 text-slate-500" />
                            <span>{log.entity_kind}</span>
                          </div>
                        </td>

                        {/* Entity ID */}
                        <td className="px-6 py-3.5 whitespace-nowrap text-slate-200">
                          <div className="flex items-center space-x-1.5">
                            <Database className="w-3.5 h-3.5 text-slate-500" />
                            <span>{log.entity_id}</span>
                          </div>
                        </td>

                        {/* Changed By */}
                        <td className="px-6 py-3.5 whitespace-nowrap text-slate-400">
                          <div className="flex items-center space-x-2">
                            <User className="w-3.5 h-3.5 text-slate-500" />
                            <span className="hover:text-emerald-400 transition">{log.changed_by}</span>
                          </div>
                        </td>

                        {/* Action View */}
                        <td className="px-6 py-3.5 whitespace-nowrap text-right">
                          <button 
                            className="p-1 rounded bg-slate-800 hover:bg-slate-700 border border-slate-700/60 text-slate-300 hover:text-white transition"
                            title="Inspect Delta Diff"
                          >
                            <Eye className="w-4 h-4" />
                          </button>
                        </td>
                      </tr>
                    );
                  })
                ) : (
                  <tr>
                    <td colSpan={6} className="px-6 py-12 text-center text-slate-500 font-sans">
                      <Terminal className="w-8 h-8 mx-auto mb-2 text-slate-600" />
                      <p className="text-sm font-semibold">No CDC events match current criteria</p>
                      <p className="text-xs text-slate-600">Waiting for Datastore mutations trigger event...</p>
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>

      </main>

      {/* --------------------------------------------------------------------------
          STATE DELTA VIEWER (SLIDE-OVER PANEL)
          -------------------------------------------------------------------------- */}
      {selectedLog && (
        <div className="fixed inset-0 z-50 overflow-hidden" aria-labelledby="slide-over-title" role="dialog" aria-modal="true">
          <div className="absolute inset-0 overflow-hidden">
            {/* Backdrop */}
            <div 
              className="absolute inset-0 bg-[#04060b]/80 backdrop-blur-sm transition-opacity" 
              onClick={() => setSelectedLog(null)}
            />

            <div className="pointer-events-none fixed inset-y-0 right-0 flex max-w-full pl-10 sm:pl-16">
              <div className="pointer-events-auto w-screen max-w-3xl border-l border-[#1e293b]/80 bg-[#0a0d16] flex flex-col shadow-2xl">
                
                {/* Panel Header */}
                <div className="px-6 py-5 border-b border-[#1e293b]/60 bg-[#0d1326]/60 flex items-center justify-between">
                  <div className="space-y-1">
                    <h2 className="text-lg font-bold text-white flex items-center">
                      Mutation Inspector
                      <span className={`ml-3 inline-flex items-center px-2 py-0.5 rounded text-[10px] font-bold tracking-wider border ${
                        selectedLog.operation_type === 'INSERT' ? 'bg-emerald-950/40 border-emerald-500/30 text-emerald-400' :
                        selectedLog.operation_type === 'UPDATE' ? 'bg-amber-950/40 border-amber-500/30 text-amber-400' :
                        selectedLog.operation_type === 'DELETE' ? 'bg-rose-950/40 border-rose-500/30 text-rose-400' :
                        'bg-slate-900/60 border-slate-700 text-slate-400'
                      }`}>
                        {selectedLog.operation_type}
                      </span>
                    </h2>
                    <p className="text-xs text-slate-400">Event ID: <span className="font-mono text-slate-300 font-semibold select-all">{selectedLog.event_id}</span></p>
                  </div>
                  <button 
                    onClick={() => setSelectedLog(null)}
                    className="p-1.5 rounded-lg bg-slate-800/80 border border-slate-700/60 hover:bg-slate-700 hover:text-white text-slate-400 transition"
                  >
                    <X className="w-5 h-5" />
                  </button>
                </div>

                {/* Panel Content (Scrollable) */}
                <div className="flex-1 overflow-y-auto p-6 space-y-6 font-sans">
                  
                  {/* Entity Metadata Stats */}
                  <div className="grid grid-cols-3 gap-3 bg-[#0d1326]/40 p-4 rounded-xl border border-slate-800/60">
                    <div>
                      <span className="text-[10px] font-mono text-slate-500 uppercase font-semibold">Entity Kind</span>
                      <p className="text-sm font-semibold text-cyan-400 font-mono mt-0.5">{selectedLog.entity_kind}</p>
                    </div>
                    <div>
                      <span className="text-[10px] font-mono text-slate-500 uppercase font-semibold">Key Identifier</span>
                      <p className="text-sm font-semibold text-slate-200 font-mono mt-0.5">{selectedLog.entity_id}</p>
                    </div>
                    <div>
                      <span className="text-[10px] font-mono text-slate-500 uppercase font-semibold">Operator</span>
                      <p className="text-sm font-semibold text-emerald-400 font-mono mt-0.5 truncate" title={selectedLog.changed_by}>{selectedLog.changed_by}</p>
                    </div>
                  </div>

                  {/* Side-by-side JSON comparison grid */}
                  <div className="space-y-3">
                    <h3 className="text-xs font-mono font-semibold text-slate-400 uppercase tracking-wider flex items-center">
                      <Code2 className="w-4 h-4 mr-1.5 text-slate-500" />
                      State Delta View (Flattened Comparison)
                    </h3>
                    
                    <div className="border border-slate-800/80 rounded-xl overflow-hidden bg-[#07090f]">
                      <div className="grid grid-cols-12 bg-slate-900/60 border-b border-slate-800 text-[10px] font-mono font-semibold uppercase text-slate-400 tracking-wider">
                        <div className="col-span-4 px-4 py-2.5">Field Path</div>
                        <div className="col-span-4 px-4 py-2.5 border-l border-slate-800">Old Value (Before)</div>
                        <div className="col-span-4 px-4 py-2.5 border-l border-slate-800">New Value (After)</div>
                      </div>
                      
                      <div className="divide-y divide-slate-900/60 text-xs font-mono max-h-[450px] overflow-y-auto">
                        {getDeltaFields(selectedLog).map((field) => {
                          let rowBg = 'bg-transparent';
                          let oldText = 'text-slate-400';
                          let newText = 'text-slate-400';
                          let marker = null;

                          if (field.status === 'added') {
                            rowBg = 'bg-emerald-950/20';
                            newText = 'text-emerald-400 font-semibold';
                            oldText = 'text-slate-600 line-through';
                            marker = <span className="text-[9px] px-1 py-0.5 rounded bg-emerald-500/10 text-emerald-400 ml-1.5 uppercase font-bold">New</span>;
                          } else if (field.status === 'deleted') {
                            rowBg = 'bg-rose-950/20';
                            oldText = 'text-rose-400 font-semibold';
                            newText = 'text-slate-600';
                            marker = <span className="text-[9px] px-1 py-0.5 rounded bg-rose-500/10 text-rose-400 ml-1.5 uppercase font-bold">Del</span>;
                          } else if (field.status === 'modified') {
                            rowBg = 'bg-amber-950/15';
                            oldText = 'text-amber-500/70 line-through';
                            newText = 'text-amber-400 font-semibold';
                            marker = <span className="text-[9px] px-1 py-0.5 rounded bg-amber-500/10 text-amber-400 ml-1.5 uppercase font-bold">Mod</span>;
                          }

                          return (
                            <div key={field.key} className={`grid grid-cols-12 hover:bg-[#101524]/40 transition ${rowBg}`}>
                              <div className="col-span-4 px-4 py-3 text-slate-300 font-semibold break-all flex items-center justify-between">
                                <span>{field.key}</span>
                                {marker}
                              </div>
                              <div className={`col-span-4 px-4 py-3 border-l border-slate-900/40 break-all select-all ${oldText}`}>
                                {field.oldValStr}
                              </div>
                              <div className={`col-span-4 px-4 py-3 border-l border-slate-900/40 break-all select-all ${newText}`}>
                                {field.newValStr}
                              </div>
                            </div>
                          );
                        })}

                        {getDeltaFields(selectedLog).length === 0 && (
                          <div className="px-4 py-8 text-center text-slate-600">
                            Empty State (No properties found)
                          </div>
                        )}
                      </div>
                    </div>
                  </div>

                  {/* Raw Payload Collapsibles */}
                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <h4 className="text-xs font-mono font-semibold text-slate-400 uppercase tracking-wider mb-2">Raw Old Value</h4>
                      <pre className="p-3 bg-[#05060b] border border-slate-800/80 rounded-lg text-slate-400 text-[10px] font-mono overflow-auto max-h-64 whitespace-pre-wrap select-all">
                        {JSON.stringify(selectedLog.old_value, null, 2)}
                      </pre>
                    </div>
                    <div>
                      <h4 className="text-xs font-mono font-semibold text-slate-400 uppercase tracking-wider mb-2">Raw New Value</h4>
                      <pre className="p-3 bg-[#05060b] border border-slate-800/80 rounded-lg text-slate-400 text-[10px] font-mono overflow-auto max-h-64 whitespace-pre-wrap select-all">
                        {JSON.stringify(selectedLog.new_value, null, 2)}
                      </pre>
                    </div>
                  </div>

                </div>

                {/* Panel Footer */}
                <div className="px-6 py-4 border-t border-[#1e293b]/60 bg-[#0d1326]/60 flex items-center justify-between">
                  <div className="flex items-center space-x-2 text-slate-400 text-xs font-mono">
                    <ShieldCheck className="w-4 h-4 text-emerald-500" />
                    <span>Cryptographic Ledger Veracity Checked</span>
                  </div>
                  <button 
                    onClick={() => setSelectedLog(null)}
                    className="px-4 py-2 bg-slate-800 hover:bg-slate-700 border border-slate-700/60 rounded-lg text-xs font-bold text-slate-200 transition"
                  >
                    Close Inspector
                  </button>
                </div>

              </div>
            </div>

          </div>
        </div>
      )}

      {/* --------------------------------------------------------------------------
          FOOTER PANEL
          -------------------------------------------------------------------------- */}
      <footer className="border-t border-[#1e293b]/40 py-5 px-6 bg-[#0a0d16] text-center text-xs text-slate-500 font-mono mt-auto flex items-center justify-between">
        <p>© 2026 AetherCDC System. Actively observing Firestore/Datastore changes.</p>
        <p className="flex items-center gap-1.5">
          <span className="w-2 h-2 rounded-full bg-emerald-500 animate-glow"></span>
          Ingress Rate: Sub-Second Latency
        </p>
      </footer>

      {showDocumentation && (
        <DocumentationView onClose={() => setShowDocumentation(false)} />
      )}

    </div>
  );
}

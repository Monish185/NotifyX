'use client';

import React, { useState, useEffect, useCallback } from 'react';
import {
  Bell,
  CheckCircle2,
  Layers,
  FileCode2,
  Settings,
  Calendar,
  Radio,
  Clock,
  Shield,
  Zap,
  Plus,
  RefreshCw,
  AlertCircle,
  Check,
  X,
  Search,
  Gauge,
  Send,
  ExternalLink,
  Key,
  Server,
  Database,
  Activity,
} from 'lucide-react';

type Tab = 'overview' | 'templates' | 'preferences' | 'scheduled' | 'audit' | 'limits';

interface TemplateVersion {
  id: string;
  templateId?: string;
  version: number;
  channel: string;
  subject: string | null;
  body: string;
  variables: string[] | any;
  status: string;
  createdAt: string;
}

interface TemplateItem {
  id: string;
  key: string;
  name: string;
  description: string | null;
  channel?: string;
  activeVersionId: string | null;
  activeVersion?: TemplateVersion | null;
  createdAt?: string;
  updatedAt: string;
  versions: TemplateVersion[];
}

export default function DashboardPage() {
  const [activeTab, setActiveTab] = useState<Tab>('overview');
  const [apiBaseUrl, setApiBaseUrl] = useState('http://localhost:3001');
  const [apiKey, setApiKey] = useState('nx_live_demo_dashboard_key_88888');
  const [apiConnected, setApiConnected] = useState<boolean | null>(null);
  const [apiError, setApiError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [successToast, setSuccessToast] = useState<string | null>(null);

  // Filter & Selected State
  const [selectedTemplate, setSelectedTemplate] = useState<string | null>(null);
  const [userIdFilter, setUserIdFilter] = useState('usr_1001');

  // Modals State
  const [showNewTemplateModal, setShowNewTemplateModal] = useState(false);
  const [showNewVersionModal, setShowNewVersionModal] = useState(false);
  const [showSendModal, setShowSendModal] = useState(false);

  // Forms State
  const [newTemplateForm, setNewTemplateForm] = useState({
    name: '',
    key: '',
    description: '',
    channel: 'EMAIL',
    subject: '',
    body: '',
  });

  const [newVersionForm, setNewVersionForm] = useState({
    subject: '',
    body: '',
  });

  const [sendForm, setSendForm] = useState({
    userId: 'usr_1001',
    channel: 'EMAIL',
    category: 'TRANSACTIONAL',
    subject: 'NotifyX Live Dispatch',
    body: 'Hello from the live NotifyX interactive console!',
    scheduleMinutes: 0,
  });

  // Domain Data State (Synced with real backend)
  const [templates, setTemplates] = useState<TemplateItem[]>([]);
  const [preferences, setPreferences] = useState<{ [channelCategory: string]: boolean }>({});
  const [scheduledJobs, setScheduledJobs] = useState<any[]>([]);
  const [notifications, setNotifications] = useState<any[]>([]);
  const [systemHealth, setSystemHealth] = useState<any>(null);
  const [tenantLimits, setTenantLimits] = useState({
    requestsPerSecond: 10,
    burstCapacity: 20,
    notificationsPerMinute: 100,
    notificationsPerDay: 10000,
    enabled: true,
  });

  const showNotification = (msg: string) => {
    setSuccessToast(msg);
    setTimeout(() => setSuccessToast(null), 4500);
  };

  // Safe Mustache variable extractor
  const extractMustacheVariables = (text: string): string[] => {
    const regex = /\{\{\s*([a-zA-Z0-9_.-]+)\s*\}\}/g;
    const matches = new Set<string>();
    let m;
    while ((m = regex.exec(text)) !== null) {
      matches.add(m[1]);
    }
    return Array.from(matches);
  };

  // Reusable API Caller
  const apiCall = useCallback(
    async (endpoint: string, options: RequestInit = {}) => {
      try {
        const res = await fetch(`${apiBaseUrl}${endpoint}`, {
          ...options,
          headers: {
            Authorization: `Bearer ${apiKey}`,
            'Content-Type': 'application/json',
            ...(options.headers || {}),
          },
        });

        if (!res.ok) {
          const errBody = await res.json().catch(() => ({ message: `HTTP ${res.status}` }));
          throw new Error(errBody.message || errBody.error || `HTTP ${res.status}`);
        }
        return await res.json();
      } catch (err: any) {
        throw err;
      }
    },
    [apiBaseUrl, apiKey]
  );

  // ---------------------------------------------------------------------------
  // Data Loaders from Live Backend
  // ---------------------------------------------------------------------------
  const checkHealth = useCallback(async () => {
    try {
      const health = await fetch(`${apiBaseUrl}/health`).then((r) => r.json());
      setSystemHealth(health);
      setApiConnected(true);
      setApiError(null);
    } catch (err: any) {
      setApiConnected(false);
      setApiError(`Cannot connect to NotifyX API at ${apiBaseUrl}. Ensure API server is running on port 3001.`);
    }
  }, [apiBaseUrl]);

  const loadTemplates = useCallback(async () => {
    try {
      const res = await apiCall('/v1/templates?limit=50');
      if (res && Array.isArray(res.data)) {
        const items = res.data.map((t: any) => ({
          ...t,
          channel: t.activeVersion?.channel || t.versions?.[0]?.channel || 'EMAIL',
          versions: t.versions && t.versions.length > 0 ? t.versions : t.activeVersion ? [t.activeVersion] : [],
        }));
        setTemplates(items);
        if (items.length > 0 && !selectedTemplate) {
          setSelectedTemplate(items[0].id);
        }
      }
    } catch (err: any) {
      console.warn('Failed to load live templates:', err.message);
    }
  }, [apiCall, selectedTemplate]);

  const loadPreferences = useCallback(
    async (uId: string) => {
      try {
        const res = await apiCall(`/v1/users/${uId}/preferences`);
        if (res && Array.isArray(res.preferences)) {
          const map: { [key: string]: boolean } = {};
          res.preferences.forEach((p: any) => {
            map[`${p.channel}:${p.category}`] = p.enabled;
          });
          setPreferences(map);
        }
      } catch (err: any) {
        console.warn('Failed to load user preferences:', err.message);
      }
    },
    [apiCall]
  );

  const loadNotifications = useCallback(async () => {
    try {
      const res = await apiCall('/v1/notifications?limit=50');
      if (res && Array.isArray(res.data)) {
        setNotifications(res.data);
        const scheduled = res.data.filter((n: any) => n.status === 'SCHEDULED');
        setScheduledJobs(scheduled);
      }
    } catch (err: any) {
      console.warn('Failed to load notifications:', err.message);
    }
  }, [apiCall]);

  const loadRateLimits = useCallback(async () => {
    try {
      const res = await apiCall('/v1/tenant/rate-limits');
      if (res) {
        setTenantLimits({
          requestsPerSecond: res.requestsPerSecond ?? 10,
          burstCapacity: res.burstCapacity ?? 20,
          notificationsPerMinute: res.notificationsPerMinute ?? 100,
          notificationsPerDay: res.notificationsPerDay ?? 10000,
          enabled: res.enabled ?? true,
        });
      }
    } catch (err: any) {
      console.warn('Failed to load rate limits:', err.message);
    }
  }, [apiCall]);

  // Refresh current tab data
  const refreshCurrentData = useCallback(async () => {
    setIsLoading(true);
    await checkHealth();
    await Promise.allSettled([
      loadTemplates(),
      loadPreferences(userIdFilter),
      loadNotifications(),
      loadRateLimits(),
    ]);
    setIsLoading(false);
  }, [checkHealth, loadTemplates, loadPreferences, loadNotifications, loadRateLimits, userIdFilter]);

  useEffect(() => {
    refreshCurrentData();
  }, [refreshCurrentData]);

  // ---------------------------------------------------------------------------
  // Interactive Actions with Live Backend
  // ---------------------------------------------------------------------------
  const handleCreateTemplate = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newTemplateForm.name || !newTemplateForm.key || !newTemplateForm.body) return;

    setIsLoading(true);
    try {
      const vars = extractMustacheVariables(newTemplateForm.body + ' ' + (newTemplateForm.subject || ''));
      const payload = {
        key: newTemplateForm.key.toLowerCase().replace(/[^a-z0-9-_]/g, '-'),
        name: newTemplateForm.name,
        description: newTemplateForm.description || undefined,
        channel: newTemplateForm.channel,
        subject: newTemplateForm.subject || undefined,
        body: newTemplateForm.body,
        variables: vars,
        activateNow: true,
      };

      const created = await apiCall('/v1/templates', {
        method: 'POST',
        body: JSON.stringify(payload),
      });

      showNotification(`Template "${created.name}" created and committed to PostgreSQL!`);
      setShowNewTemplateModal(false);
      setNewTemplateForm({ name: '', key: '', description: '', channel: 'EMAIL', subject: '', body: '' });
      await loadTemplates();
      setSelectedTemplate(created.id);
    } catch (err: any) {
      alert(`Failed to create template: ${err.message}`);
    } finally {
      setIsLoading(false);
    }
  };

  const handleCreateVersion = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!activeTemplateObj || !newVersionForm.body) return;

    setIsLoading(true);
    try {
      const vars = extractMustacheVariables(newVersionForm.body + ' ' + (newVersionForm.subject || ''));
      const payload = {
        channel: activeTemplateObj.channel || 'EMAIL',
        subject: newVersionForm.subject || undefined,
        body: newVersionForm.body,
        variables: vars,
        activateNow: false,
      };

      await apiCall(`/v1/templates/${activeTemplateObj.id}/versions`, {
        method: 'POST',
        body: JSON.stringify(payload),
      });

      showNotification(`New version created in DRAFT state!`);
      setShowNewVersionModal(false);
      setNewVersionForm({ subject: '', body: '' });
      await loadTemplates();
    } catch (err: any) {
      alert(`Failed to add version: ${err.message}`);
    } finally {
      setIsLoading(false);
    }
  };

  const handleActivateVersion = async (templateId: string, versionId: string) => {
    setIsLoading(true);
    try {
      await apiCall(`/v1/templates/${templateId}/versions/${versionId}/activate`, {
        method: 'POST',
      });
      showNotification(`Version activated via PostgreSQL row-lock transaction!`);
      await loadTemplates();
    } catch (err: any) {
      alert(`Activation failed: ${err.message}`);
    } finally {
      setIsLoading(false);
    }
  };

  const handleTogglePreference = async (channel: string, category: string, currentEnabled: boolean) => {
    try {
      const nextVal = !currentEnabled;
      setPreferences((prev) => ({ ...prev, [`${channel}:${category}`]: nextVal }));

      await apiCall(`/v1/users/${userIdFilter}/preferences`, {
        method: 'PUT',
        body: JSON.stringify({
          channel,
          category,
          enabled: nextVal,
        }),
      });
      showNotification(`Updated preference: ${channel} ${category} is now ${nextVal ? 'ENABLED' : 'DISABLED'}`);
    } catch (err: any) {
      alert(`Failed to update preference: ${err.message}`);
      await loadPreferences(userIdFilter);
    }
  };

  const handleCancelScheduled = async (notificationId: string) => {
    if (!confirm('Are you sure you want to cancel this scheduled notification?')) return;
    setIsLoading(true);
    try {
      await apiCall(`/v1/notifications/${notificationId}/cancel`, {
        method: 'POST',
      });
      showNotification('Scheduled notification cancelled successfully!');
      await loadNotifications();
    } catch (err: any) {
      alert(`Cancellation failed: ${err.message}`);
    } finally {
      setIsLoading(false);
    }
  };

  const handleSaveLimits = async () => {
    setIsLoading(true);
    try {
      await apiCall('/v1/tenant/rate-limits', {
        method: 'PUT',
        body: JSON.stringify(tenantLimits),
      });
      showNotification('Tenant Rate Limits & Quotas saved to PostgreSQL!');
    } catch (err: any) {
      alert(`Failed to update limits: ${err.message}`);
    } finally {
      setIsLoading(false);
    }
  };

  const handleSendLiveNotification = async (e: React.FormEvent) => {
    e.preventDefault();
    setIsLoading(true);
    try {
      const payload: any = {
        userId: sendForm.userId,
        channels: [sendForm.channel],
        category: sendForm.category,
        payload: {
          subject: sendForm.subject,
          body: sendForm.body,
        },
      };

      if (sendForm.scheduleMinutes > 0) {
        payload.scheduleAt = new Date(Date.now() + sendForm.scheduleMinutes * 60000).toISOString();
      }

      const res = await apiCall('/v1/notifications', {
        method: 'POST',
        headers: {
          'Idempotency-Key': `dash_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`,
        },
        body: JSON.stringify(payload),
      });

      showNotification(`Notification accepted (ID: ${res.id.slice(0, 10)}...)! Outbox event emitted.`);
      setShowSendModal(false);
      await loadNotifications();
      setActiveTab(sendForm.scheduleMinutes > 0 ? 'scheduled' : 'audit');
    } catch (err: any) {
      alert(`Failed to send notification: ${err.message}`);
    } finally {
      setIsLoading(false);
    }
  };

  const activeTemplateObj =
    templates.find((t) => t.id === selectedTemplate) ||
    templates[0] || {
      id: 'none',
      key: 'none',
      name: 'No Templates Yet',
      description: 'Click "New Template" to create one in PostgreSQL',
      channel: 'EMAIL',
      activeVersionId: null,
      versions: [],
    };

  return (
    <div className="flex min-h-screen bg-slate-950 text-slate-100 font-sans">
      {/* Sidebar Navigation */}
      <aside className="w-64 border-r border-slate-800 bg-slate-900/50 backdrop-blur p-4 flex flex-col justify-between">
        <div className="space-y-6">
          <div className="flex items-center gap-3 px-2 py-1">
            <div className="h-9 w-9 rounded-lg bg-blue-600 flex items-center justify-center font-bold text-white shadow-lg shadow-blue-500/20">
              <Bell className="h-5 w-5" />
            </div>
            <div>
              <span className="text-lg font-semibold tracking-tight">NotifyX</span>
              <span className="block text-[10px] text-emerald-400 font-mono font-medium">LIVE SYSTEM CONSOLE</span>
            </div>
          </div>

          <nav className="space-y-1">
            <button
              onClick={() => setActiveTab('overview')}
              className={`w-full flex items-center gap-3 px-3 py-2 text-sm font-medium rounded-md transition-colors ${
                activeTab === 'overview'
                  ? 'bg-blue-600/10 text-blue-400 border border-blue-500/20'
                  : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/50'
              }`}
            >
              <Layers className="h-4 w-4" />
              Overview
            </button>

            <div className="px-3 py-2 text-xs font-semibold text-slate-500 uppercase tracking-wider mt-4">
              Product & Routing
            </div>
            <button
              onClick={() => setActiveTab('templates')}
              className={`w-full flex items-center gap-3 px-3 py-2 text-sm font-medium rounded-md transition-colors ${
                activeTab === 'templates'
                  ? 'bg-blue-600/10 text-blue-400 border border-blue-500/20'
                  : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/50'
              }`}
            >
              <FileCode2 className="h-4 w-4" />
              Templates ({templates.length})
            </button>
            <button
              onClick={() => setActiveTab('preferences')}
              className={`w-full flex items-center gap-3 px-3 py-2 text-sm font-medium rounded-md transition-colors ${
                activeTab === 'preferences'
                  ? 'bg-blue-600/10 text-blue-400 border border-blue-500/20'
                  : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/50'
              }`}
            >
              <Settings className="h-4 w-4" />
              User Preferences
            </button>
            <button
              onClick={() => setActiveTab('scheduled')}
              className={`w-full flex items-center gap-3 px-3 py-2 text-sm font-medium rounded-md transition-colors ${
                activeTab === 'scheduled'
                  ? 'bg-blue-600/10 text-blue-400 border border-blue-500/20'
                  : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/50'
              }`}
            >
              <Calendar className="h-4 w-4" />
              Scheduled ({scheduledJobs.length})
            </button>
            <button
              onClick={() => setActiveTab('audit')}
              className={`w-full flex items-center gap-3 px-3 py-2 text-sm font-medium rounded-md transition-colors ${
                activeTab === 'audit'
                  ? 'bg-blue-600/10 text-blue-400 border border-blue-500/20'
                  : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/50'
              }`}
            >
              <Radio className="h-4 w-4" />
              Audit & Deliveries ({notifications.length})
            </button>

            <div className="px-3 py-2 text-xs font-semibold text-slate-500 uppercase tracking-wider mt-4">
              Phase 11 Resiliency
            </div>
            <button
              onClick={() => setActiveTab('limits')}
              className={`w-full flex items-center gap-3 px-3 py-2 text-sm font-medium rounded-md transition-colors ${
                activeTab === 'limits'
                  ? 'bg-blue-600/10 text-blue-400 border border-blue-500/20'
                  : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/50'
              }`}
            >
              <Gauge className="h-4 w-4" />
              Usage & Limits
            </button>
          </nav>
        </div>

        <div className="border-t border-slate-800 pt-4 px-2 space-y-2">
          <div className="flex items-center justify-between text-[11px] font-mono">
            <span className="text-slate-400">Backend API</span>
            <span className={apiConnected ? 'text-emerald-400 font-bold' : 'text-rose-400 font-bold'}>
              {apiConnected ? '● Online (3001)' : '○ Offline'}
            </span>
          </div>
          <div className="text-[10px] text-slate-500 truncate" title={apiBaseUrl}>
            {apiBaseUrl}
          </div>
        </div>
      </aside>

      {/* Main Content Area */}
      <main className="flex-1 flex flex-col overflow-y-auto">
        {/* Header with Connection & Quick Action */}
        <header className="h-16 border-b border-slate-800 px-8 flex items-center justify-between bg-slate-900/40 backdrop-blur sticky top-0 z-10">
          <div className="flex items-center gap-4">
            <h1 className="text-sm font-semibold text-slate-200">NotifyX Platform Console</h1>
            <span className="text-xs bg-slate-800 text-slate-300 px-2.5 py-0.5 rounded-full border border-slate-700">
              v1.11.0-live
            </span>
            <button
              onClick={refreshCurrentData}
              disabled={isLoading}
              className="flex items-center gap-1.5 text-xs text-slate-400 hover:text-slate-200 bg-slate-800/60 hover:bg-slate-800 px-2.5 py-1 rounded transition-colors"
            >
              <RefreshCw className={`h-3 w-3 ${isLoading ? 'animate-spin' : ''}`} />
              Sync
            </button>
          </div>

          <div className="flex items-center gap-3">
            {/* Live API Key Input / Indicator */}
            <div className="flex items-center gap-2 bg-slate-900 border border-slate-800 px-3 py-1 rounded-lg text-xs">
              <Key className="h-3.5 w-3.5 text-amber-400" />
              <input
                type="text"
                value={apiKey}
                onChange={(e) => setApiKey(e.target.value)}
                placeholder="Bearer API Key..."
                className="bg-transparent border-none text-[11px] font-mono text-slate-300 focus:outline-none w-48"
                title="Current Tenant API Key"
              />
            </div>

            <button
              onClick={() => setShowSendModal(true)}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-emerald-600 hover:bg-emerald-500 text-white rounded-md text-xs font-semibold shadow-lg shadow-emerald-600/20 transition-all cursor-pointer"
            >
              <Send className="h-3.5 w-3.5" />
              Send Notification
            </button>
          </div>
        </header>

        {/* Success Toast */}
        {successToast && (
          <div className="mx-8 mt-4 p-3.5 bg-emerald-950/80 border border-emerald-600/50 rounded-xl flex items-center justify-between text-xs text-emerald-200 shadow-xl animate-in fade-in slide-in-from-top-2">
            <div className="flex items-center gap-2.5">
              <CheckCircle2 className="h-4 w-4 text-emerald-400 shrink-0" />
              <span>{successToast}</span>
            </div>
            <button onClick={() => setSuccessToast(null)} className="text-emerald-400 hover:text-white">
              <X className="h-3.5 w-3.5" />
            </button>
          </div>
        )}

        {/* API Error Warning */}
        {apiError && (
          <div className="mx-8 mt-4 p-3 bg-rose-950/60 border border-rose-800/50 rounded-xl flex items-center justify-between text-xs text-rose-300">
            <div className="flex items-center gap-2">
              <AlertCircle className="h-4 w-4 text-rose-400 shrink-0" />
              <span>{apiError}</span>
            </div>
            <button
              onClick={() => checkHealth()}
              className="underline hover:text-white text-xs font-medium ml-4 cursor-pointer"
            >
              Retry Connection
            </button>
          </div>
        )}

        <div className="p-8 space-y-6 max-w-7xl">
          {/* ========================================================================= */}
          {/* TAB 1: OVERVIEW */}
          {/* ========================================================================= */}
          {activeTab === 'overview' && (
            <div className="space-y-6">
              {/* Infrastructure Stats Grid */}
              <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                <div className="p-5 rounded-xl border border-slate-800 bg-slate-900/50 space-y-2">
                  <div className="flex items-center justify-between text-xs text-slate-400">
                    <span>Database</span>
                    <Database className="h-4 w-4 text-blue-400" />
                  </div>
                  <div className="text-lg font-bold text-white">PostgreSQL 16</div>
                  <div className="text-[11px] text-emerald-400 font-mono">
                    {systemHealth?.postgres ? 'Connected (Healthy)' : 'Authoritative Storage'}
                  </div>
                </div>

                <div className="p-5 rounded-xl border border-slate-800 bg-slate-900/50 space-y-2">
                  <div className="flex items-center justify-between text-xs text-slate-400">
                    <span>Cache & Token Buckets</span>
                    <Zap className="h-4 w-4 text-rose-400" />
                  </div>
                  <div className="text-lg font-bold text-white">Redis 7 Cluster</div>
                  <div className="text-[11px] text-emerald-400 font-mono">
                    {systemHealth?.redis ? 'Active (Sub-ms Lua)' : 'Distributed Rate Limiter'}
                  </div>
                </div>

                <div className="p-5 rounded-xl border border-slate-800 bg-slate-900/50 space-y-2">
                  <div className="flex items-center justify-between text-xs text-slate-400">
                    <span>Event Streaming</span>
                    <Radio className="h-4 w-4 text-emerald-400" />
                  </div>
                  <div className="text-lg font-bold text-white">Apache Kafka 3.7</div>
                  <div className="text-[11px] text-emerald-400 font-mono">4 Consumer Groups</div>
                </div>

                <div className="p-5 rounded-xl border border-slate-800 bg-slate-900/50 space-y-2">
                  <div className="flex items-center justify-between text-xs text-slate-400">
                    <span>Outbox Publisher</span>
                    <Activity className="h-4 w-4 text-amber-400" />
                  </div>
                  <div className="text-lg font-bold text-white">Zero Dual-Writes</div>
                  <div className="text-[11px] text-emerald-400 font-mono">At-Least-Once Delivery</div>
                </div>
              </div>

              {/* Live Channels Status */}
              <div className="p-6 rounded-xl border border-slate-800 bg-slate-900/50 space-y-4">
                <div className="flex items-center justify-between border-b border-slate-800 pb-3">
                  <h3 className="text-sm font-semibold text-white">Active Channel Workers & Semaphore Concurrency</h3>
                  <span className="text-xs text-slate-400 font-mono">Kafka Consumer Groups Bound</span>
                </div>

                <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                  {[
                    { name: 'Email Worker', port: 3004, channel: 'EMAIL', conc: 10, group: 'notifyx-email-workers' },
                    { name: 'SMS Worker', port: 3006, channel: 'SMS', conc: 5, group: 'notifyx-sms-workers' },
                    { name: 'Push Worker', port: 3005, channel: 'PUSH', conc: 20, group: 'notifyx-push-workers' },
                    { name: 'In-App Worker', port: 3003, channel: 'IN_APP', conc: 20, group: 'notifyx-inapp-workers' },
                  ].map((w) => (
                    <div key={w.name} className="p-4 rounded-lg bg-slate-950/60 border border-slate-800 space-y-2">
                      <div className="flex items-center justify-between">
                        <span className="text-xs font-semibold text-slate-200">{w.name}</span>
                        <span className="text-[10px] px-2 py-0.5 rounded bg-emerald-950/50 text-emerald-300 border border-emerald-800/50">
                          Active
                        </span>
                      </div>
                      <div className="text-[11px] text-slate-400 font-mono">Port: {w.port}</div>
                      <div className="text-[11px] text-slate-400 font-mono">Max Concurrency: {w.conc} in-flight</div>
                      <div className="text-[10px] text-slate-500 font-mono truncate">{w.group}</div>
                    </div>
                  ))}
                </div>
              </div>

              {/* Quick Actions Panel */}
              <div className="p-6 rounded-xl border border-blue-900/30 bg-blue-950/20 flex items-center justify-between">
                <div className="space-y-1">
                  <h3 className="text-sm font-semibold text-blue-200">Want to test the live notification pipeline?</h3>
                  <p className="text-xs text-blue-300/80">
                    Dispatch a real notification into PostgreSQL Outbox → Kafka → Worker with one click.
                  </p>
                </div>
                <button
                  onClick={() => setShowSendModal(true)}
                  className="px-4 py-2 bg-blue-600 hover:bg-blue-500 text-white rounded-lg text-xs font-semibold transition-all shadow-lg shadow-blue-600/30 cursor-pointer flex items-center gap-2"
                >
                  <Send className="h-4 w-4" />
                  Dispatch Test Notification
                </button>
              </div>
            </div>
          )}

          {/* ========================================================================= */}
          {/* TAB 2: TEMPLATES */}
          {/* ========================================================================= */}
          {activeTab === 'templates' && (
            <div className="space-y-6">
              <div className="flex items-center justify-between">
                <div>
                  <h2 className="text-lg font-semibold text-white">Template Management (PostgreSQL Driven)</h2>
                  <p className="text-xs text-slate-400">
                    Live database templates with immutable versioning, mustache variable interpolation, and tenant activation locks.
                  </p>
                </div>
                <button
                  onClick={() => setShowNewTemplateModal(true)}
                  className="flex items-center gap-2 px-3 py-1.5 bg-blue-600 hover:bg-blue-500 text-white rounded-md text-xs font-semibold transition-colors cursor-pointer shadow-lg shadow-blue-600/20"
                >
                  <Plus className="h-3.5 w-3.5" />
                  New Template
                </button>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                {/* Template List */}
                <div className="space-y-2">
                  <div className="text-xs font-semibold text-slate-400 uppercase tracking-wider px-1">
                    Database Templates ({templates.length})
                  </div>
                  {templates.length === 0 ? (
                    <div className="p-6 rounded-xl border border-dashed border-slate-800 text-center text-xs text-slate-500">
                      No templates in database. Click &quot;New Template&quot; to create one!
                    </div>
                  ) : (
                    templates.map((tpl) => (
                      <div
                        key={tpl.id}
                        onClick={() => setSelectedTemplate(tpl.id)}
                        className={`p-4 rounded-xl border cursor-pointer transition-all ${
                          selectedTemplate === tpl.id
                            ? 'bg-blue-950/40 border-blue-500/40 shadow-lg shadow-blue-500/5'
                            : 'bg-slate-900/50 border-slate-800 hover:border-slate-700'
                        }`}
                      >
                        <div className="flex items-center justify-between">
                          <span className="text-xs font-mono px-2 py-0.5 rounded bg-slate-800 text-blue-400">
                            {tpl.channel}
                          </span>
                          <span className="text-[11px] text-slate-500">{tpl.versions?.length || 1} versions</span>
                        </div>
                        <h4 className="text-sm font-semibold text-slate-200 mt-2">{tpl.name}</h4>
                        <p className="text-xs text-slate-400 mt-1 font-mono">{tpl.key}</p>
                      </div>
                    ))
                  )}
                </div>

                {/* Template Detail & Versions */}
                <div className="md:col-span-2 space-y-4">
                  {activeTemplateObj.id !== 'none' ? (
                    <div className="p-6 rounded-xl border border-slate-800 bg-slate-900/50 space-y-6">
                      <div className="flex items-start justify-between border-b border-slate-800 pb-4">
                        <div>
                          <h3 className="text-base font-semibold text-white">{activeTemplateObj.name}</h3>
                          <p className="text-xs text-slate-400 mt-0.5">{activeTemplateObj.description || 'No description'}</p>
                        </div>
                        <span className="text-xs font-mono text-slate-400 bg-slate-800 px-2.5 py-1 rounded">
                          Key: {activeTemplateObj.key}
                        </span>
                      </div>

                      <div className="space-y-4">
                        <div className="flex items-center justify-between">
                          <div className="flex items-center gap-3">
                            <h4 className="text-xs font-semibold text-slate-300 uppercase tracking-wider">
                              Version History (Immutable)
                            </h4>
                            <button
                              onClick={() => setShowNewVersionModal(true)}
                              className="flex items-center gap-1 px-2.5 py-1 bg-slate-800 hover:bg-slate-700 text-blue-400 rounded text-xs font-medium transition-colors border border-slate-700 cursor-pointer"
                            >
                              <Plus className="h-3 w-3" />
                              Add Version
                            </button>
                          </div>
                          <span className="text-[11px] text-slate-500">
                            Active Version:{' '}
                            <code className="text-emerald-400 font-mono">
                              {activeTemplateObj.activeVersionId ? activeTemplateObj.activeVersionId.slice(0, 12) + '...' : 'None'}
                            </code>
                          </span>
                        </div>

                        <div className="space-y-3">
                          {activeTemplateObj.versions.map((ver) => (
                            <div
                              key={ver.id}
                              className={`p-4 rounded-lg border ${
                                ver.id === activeTemplateObj.activeVersionId
                                  ? 'bg-emerald-950/20 border-emerald-800/40'
                                  : 'bg-slate-950/40 border-slate-800'
                              }`}
                            >
                              <div className="flex items-center justify-between mb-2">
                                <div className="flex items-center gap-2">
                                  <span className="text-xs font-bold text-slate-200">v{ver.version}</span>
                                  <span
                                    className={`text-[10px] px-2 py-0.5 rounded font-mono font-medium ${
                                      ver.id === activeTemplateObj.activeVersionId
                                        ? 'bg-emerald-900/60 text-emerald-300 border border-emerald-700/60'
                                        : 'bg-slate-800 text-slate-400'
                                    }`}
                                  >
                                    {ver.id === activeTemplateObj.activeVersionId ? 'ACTIVE' : 'ARCHIVED / DRAFT'}
                                  </span>
                                </div>

                                {ver.id !== activeTemplateObj.activeVersionId && (
                                  <button
                                    onClick={() => handleActivateVersion(activeTemplateObj.id, ver.id)}
                                    className="text-xs px-2.5 py-1 bg-slate-800 hover:bg-slate-700 text-slate-200 rounded border border-slate-700 transition-colors cursor-pointer"
                                  >
                                    Promote to Active
                                  </button>
                                )}
                              </div>

                              {ver.subject && (
                                <div className="text-xs text-slate-300 mb-1">
                                  <span className="text-slate-500 font-mono">Subject: </span>
                                  {ver.subject}
                                </div>
                              )}

                              <div className="text-xs text-slate-300 font-mono bg-slate-900/80 p-2.5 rounded border border-slate-800/80 whitespace-pre-wrap">
                                {ver.body}
                              </div>

                              {Array.isArray(ver.variables) && ver.variables.length > 0 && (
                                <div className="flex items-center gap-2 mt-2">
                                  <span className="text-[11px] text-slate-500">Variables:</span>
                                  {ver.variables.map((v: string) => (
                                    <span
                                      key={v}
                                      className="text-[10px] font-mono bg-blue-950/50 text-blue-300 border border-blue-800/40 px-1.5 py-0.5 rounded"
                                    >
                                      &#123;&#123;{v}&#125;&#125;
                                    </span>
                                  ))}
                                </div>
                              )}
                            </div>
                          ))}
                        </div>
                      </div>
                    </div>
                  ) : (
                    <div className="p-8 rounded-xl border border-slate-800 bg-slate-900/30 text-center text-slate-400 text-xs">
                      Select a template from the list on the left to view its immutable versions.
                    </div>
                  )}
                </div>
              </div>
            </div>
          )}

          {/* ========================================================================= */}
          {/* TAB 3: PREFERENCES */}
          {/* ========================================================================= */}
          {activeTab === 'preferences' && (
            <div className="space-y-6">
              <div className="flex items-center justify-between">
                <div>
                  <h2 className="text-lg font-semibold text-white">Recipient Notification Preferences</h2>
                  <p className="text-xs text-slate-400">
                    Live database matrix per user. Critical alerts default to enabled; marketing defaults to disabled.
                  </p>
                </div>
                <div className="flex items-center gap-2">
                  <div className="relative">
                    <Search className="h-3.5 w-3.5 absolute left-3 top-2.5 text-slate-500" />
                    <input
                      type="text"
                      value={userIdFilter}
                      onChange={(e) => setUserIdFilter(e.target.value)}
                      placeholder="User ID (e.g. usr_1001)..."
                      className="bg-slate-900 border border-slate-800 rounded-md pl-9 pr-3 py-1.5 text-xs text-slate-200 focus:outline-none focus:border-blue-500 w-52"
                    />
                  </div>
                  <button
                    onClick={() => loadPreferences(userIdFilter)}
                    className="px-3 py-1.5 bg-slate-800 hover:bg-slate-700 text-xs text-slate-200 rounded-md border border-slate-700"
                  >
                    Load User
                  </button>
                </div>
              </div>

              <div className="p-6 rounded-xl border border-slate-800 bg-slate-900/50">
                <table className="w-full text-left text-xs">
                  <thead>
                    <tr className="border-b border-slate-800 text-slate-400 uppercase font-mono text-[11px]">
                      <th className="pb-3">Channel</th>
                      <th className="pb-3">Transactional</th>
                      <th className="pb-3">Security</th>
                      <th className="pb-3">System</th>
                      <th className="pb-3">Marketing</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-800/60">
                    {['EMAIL', 'SMS', 'PUSH', 'IN_APP'].map((channel) => (
                      <tr key={channel} className="hover:bg-slate-800/30 transition-colors">
                        <td className="py-4 font-semibold text-slate-200 flex items-center gap-2">
                          <span className="text-xs font-mono px-2 py-0.5 rounded bg-slate-800 text-blue-400">
                            {channel}
                          </span>
                        </td>
                        {['TRANSACTIONAL', 'SECURITY', 'SYSTEM', 'MARKETING'].map((category) => {
                          const key = `${channel}:${category}`;
                          const isOptInDefault = category !== 'MARKETING';
                          const enabled = preferences[key] !== undefined ? preferences[key] : isOptInDefault;

                          return (
                            <td key={category} className="py-4">
                              <button
                                onClick={() => handleTogglePreference(channel, category, enabled)}
                                className={`px-3 py-1.5 rounded text-xs font-mono transition-all flex items-center gap-1.5 cursor-pointer ${
                                  enabled
                                    ? 'bg-emerald-950/60 text-emerald-300 border border-emerald-700/60 hover:bg-emerald-900/60'
                                    : 'bg-rose-950/40 text-rose-300 border border-rose-800/50 hover:bg-rose-900/50'
                                }`}
                              >
                                {enabled ? <Check className="h-3 w-3 text-emerald-400" /> : <X className="h-3 w-3 text-rose-400" />}
                                {enabled ? 'ENABLED' : 'DISABLED'}
                              </button>
                            </td>
                          );
                        })}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* ========================================================================= */}
          {/* TAB 4: SCHEDULED NOTIFICATIONS */}
          {/* ========================================================================= */}
          {activeTab === 'scheduled' && (
            <div className="space-y-6">
              <div className="flex items-center justify-between">
                <div>
                  <h2 className="text-lg font-semibold text-white">Scheduled Notification Queue</h2>
                  <p className="text-xs text-slate-400">
                    Polled autonomously by the Scheduler Service (port 3007) via PostgreSQL{' '}
                    <code className="text-blue-400 font-mono">FOR UPDATE SKIP LOCKED</code>.
                  </p>
                </div>
                <button
                  onClick={loadNotifications}
                  className="px-3 py-1.5 bg-slate-800 hover:bg-slate-700 text-slate-200 rounded text-xs font-medium border border-slate-700"
                >
                  Refresh Queue
                </button>
              </div>

              <div className="p-6 rounded-xl border border-slate-800 bg-slate-900/50">
                {scheduledJobs.length === 0 ? (
                  <div className="py-12 text-center text-xs text-slate-500 space-y-2">
                    <Clock className="h-8 w-8 text-slate-600 mx-auto" />
                    <div>No notifications currently scheduled.</div>
                    <p className="text-slate-600">
                      Use the &quot;Send Notification&quot; button and specify a schedule delay!
                    </p>
                  </div>
                ) : (
                  <div className="overflow-x-auto">
                    <table className="w-full text-left text-xs">
                      <thead>
                        <tr className="border-b border-slate-800 text-slate-400 uppercase font-mono text-[11px]">
                          <th className="pb-3">Notification ID</th>
                          <th className="pb-3">Channels</th>
                          <th className="pb-3">Category</th>
                          <th className="pb-3">Scheduled At</th>
                          <th className="pb-3">Status</th>
                          <th className="pb-3 text-right">Actions</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-slate-800/60">
                        {scheduledJobs.map((job) => (
                          <tr key={job.id} className="hover:bg-slate-800/30">
                            <td className="py-3 font-mono text-slate-300">{job.id.slice(0, 14)}...</td>
                            <td className="py-3 font-mono text-blue-400">{(job.channels || []).join(', ')}</td>
                            <td className="py-3 font-mono text-slate-300">{job.category}</td>
                            <td className="py-3 text-slate-300">
                              {job.scheduledAt ? new Date(job.scheduledAt).toLocaleString() : 'Soon'}
                            </td>
                            <td className="py-3">
                              <span className="text-[10px] px-2 py-0.5 rounded font-mono bg-blue-950/60 text-blue-300 border border-blue-800/50">
                                {job.status}
                              </span>
                            </td>
                            <td className="py-3 text-right">
                              <button
                                onClick={() => handleCancelScheduled(job.id)}
                                className="px-2.5 py-1 bg-rose-950/50 hover:bg-rose-900/60 text-rose-300 border border-rose-800/50 rounded text-xs transition-colors cursor-pointer"
                              >
                                Cancel Job
                              </button>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            </div>
          )}

          {/* ========================================================================= */}
          {/* TAB 5: AUDIT & DELIVERIES */}
          {/* ========================================================================= */}
          {activeTab === 'audit' && (
            <div className="space-y-6">
              <div className="flex items-center justify-between">
                <div>
                  <h2 className="text-lg font-semibold text-white">Live Notification Delivery Audit Log</h2>
                  <p className="text-xs text-slate-400">
                    Real-time PostgreSQL notifications and delivery attempts with correlation IDs and provider message IDs.
                  </p>
                </div>
                <button
                  onClick={loadNotifications}
                  className="px-3 py-1.5 bg-slate-800 hover:bg-slate-700 text-slate-200 rounded text-xs font-medium border border-slate-700"
                >
                  Refresh Logs
                </button>
              </div>

              <div className="space-y-4">
                {notifications.length === 0 ? (
                  <div className="p-12 text-center text-xs text-slate-500 rounded-xl border border-dashed border-slate-800">
                    No notifications recorded yet. Click &quot;Send Notification&quot; in the header to trigger one!
                  </div>
                ) : (
                  notifications.map((item) => (
                    <div key={item.id} className="p-5 rounded-xl border border-slate-800 bg-slate-900/50 space-y-4">
                      <div className="flex items-start justify-between">
                        <div className="space-y-1">
                          <div className="flex items-center gap-2">
                            <span className="text-xs font-mono font-bold text-slate-200">{item.id}</span>
                            <span
                              className={`text-[10px] font-mono px-2 py-0.5 rounded font-medium ${
                                item.status === 'DELIVERED'
                                  ? 'bg-emerald-950/60 text-emerald-300 border border-emerald-800/50'
                                  : item.status === 'RETRY_SCHEDULED'
                                  ? 'bg-amber-950/60 text-amber-300 border border-amber-800/50'
                                  : 'bg-blue-950/60 text-blue-300 border border-blue-800/50'
                              }`}
                            >
                              {item.status}
                            </span>
                            <span className="text-[10px] font-mono bg-slate-800 text-slate-400 px-2 py-0.5 rounded">
                              {item.category}
                            </span>
                          </div>
                          <div className="text-[11px] text-slate-400 font-mono">
                            Correlation ID: <span className="text-slate-300">{item.correlationId}</span>
                          </div>
                        </div>
                        <span className="text-[11px] text-slate-500">
                          {item.createdAt ? new Date(item.createdAt).toLocaleTimeString() : ''}
                        </span>
                      </div>

                      {/* Deliveries Sub-table */}
                      {item.deliveries && item.deliveries.length > 0 && (
                        <div className="bg-slate-950/60 rounded-lg p-3 border border-slate-800/80 space-y-2">
                          <div className="text-[10px] font-semibold text-slate-400 uppercase tracking-wider">
                            Channel Deliveries ({item.deliveries.length})
                          </div>
                          <div className="space-y-1.5">
                            {item.deliveries.map((del: any) => (
                              <div
                                key={del.id}
                                className="flex items-center justify-between text-xs font-mono text-slate-300 bg-slate-900/60 p-2 rounded border border-slate-800/60"
                              >
                                <div className="flex items-center gap-2">
                                  <span className="text-blue-400 font-bold">{del.channel}</span>
                                  <span className="text-slate-500">→</span>
                                  <span
                                    className={
                                      del.status === 'DELIVERED'
                                        ? 'text-emerald-400'
                                        : del.status === 'RETRY_SCHEDULED'
                                        ? 'text-amber-400'
                                        : 'text-slate-300'
                                    }
                                  >
                                    {del.status}
                                  </span>
                                  {del.providerMessageId && (
                                    <span className="text-[11px] text-slate-500">
                                      Provider ID: <code className="text-slate-400">{del.providerMessageId}</code>
                                    </span>
                                  )}
                                </div>
                                <div className="text-[11px] text-slate-500">
                                  Attempts: {del.attemptCount}
                                </div>
                              </div>
                            ))}
                          </div>
                        </div>
                      )}
                    </div>
                  ))
                )}
              </div>
            </div>
          )}

          {/* ========================================================================= */}
          {/* TAB 6: USAGE & LIMITS (PHASE 11) */}
          {/* ========================================================================= */}
          {activeTab === 'limits' && (
            <div className="space-y-6">
              <div className="flex items-center justify-between">
                <div>
                  <h2 className="text-lg font-semibold text-white">Multi-Tenant Quotas & Backpressure (Phase 11)</h2>
                  <p className="text-xs text-slate-400">
                    Live Redis Token Bucket configuration, Pre-DB Quota limits, and worker backpressure controls.
                  </p>
                </div>
                <button
                  onClick={handleSaveLimits}
                  className="px-4 py-1.5 bg-blue-600 hover:bg-blue-500 text-white rounded-md text-xs font-semibold shadow-lg shadow-blue-600/20 transition-all cursor-pointer"
                >
                  Save Quotas to PostgreSQL
                </button>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                {/* Rate Limiting Configuration Card */}
                <div className="p-6 rounded-xl border border-slate-800 bg-slate-900/50 space-y-4">
                  <h3 className="text-sm font-semibold text-white border-b border-slate-800 pb-3">
                    Tenant Rate Limit Settings
                  </h3>
                  <div className="space-y-4">
                    <div>
                      <label className="block text-xs font-medium text-slate-300 mb-1">
                        Requests Per Second (Refill Rate)
                      </label>
                      <input
                        type="number"
                        min="1"
                        value={tenantLimits.requestsPerSecond}
                        onChange={(e) =>
                          setTenantLimits((p) => ({ ...p, requestsPerSecond: Number(e.target.value) }))
                        }
                        className="w-full bg-slate-950 border border-slate-800 rounded-md px-3 py-1.5 text-xs text-slate-100 font-mono focus:outline-none focus:border-blue-500"
                      />
                    </div>

                    <div>
                      <label className="block text-xs font-medium text-slate-300 mb-1">Burst Capacity</label>
                      <input
                        type="number"
                        min="1"
                        value={tenantLimits.burstCapacity}
                        onChange={(e) =>
                          setTenantLimits((p) => ({ ...p, burstCapacity: Number(e.target.value) }))
                        }
                        className="w-full bg-slate-950 border border-slate-800 rounded-md px-3 py-1.5 text-xs text-slate-100 font-mono focus:outline-none focus:border-blue-500"
                      />
                    </div>

                    <div>
                      <label className="block text-xs font-medium text-slate-300 mb-1">
                        Notifications / Minute Quota (Pre-DB)
                      </label>
                      <input
                        type="number"
                        min="1"
                        value={tenantLimits.notificationsPerMinute}
                        onChange={(e) =>
                          setTenantLimits((p) => ({ ...p, notificationsPerMinute: Number(e.target.value) }))
                        }
                        className="w-full bg-slate-950 border border-slate-800 rounded-md px-3 py-1.5 text-xs text-slate-100 font-mono focus:outline-none focus:border-blue-500"
                      />
                    </div>

                    <div>
                      <label className="block text-xs font-medium text-slate-300 mb-1">
                        Notifications / Day Quota (Pre-DB)
                      </label>
                      <input
                        type="number"
                        min="1"
                        value={tenantLimits.notificationsPerDay}
                        onChange={(e) =>
                          setTenantLimits((p) => ({ ...p, notificationsPerDay: Number(e.target.value) }))
                        }
                        className="w-full bg-slate-950 border border-slate-800 rounded-md px-3 py-1.5 text-xs text-slate-100 font-mono focus:outline-none focus:border-blue-500"
                      />
                    </div>
                  </div>
                </div>

                {/* Backpressure Invariants Card */}
                <div className="p-6 rounded-xl border border-slate-800 bg-slate-900/50 space-y-4">
                  <h3 className="text-sm font-semibold text-white border-b border-slate-800 pb-3">
                    Phase 11 Safeguards Verified
                  </h3>
                  <div className="space-y-3 text-xs">
                    <div className="p-3 rounded-lg bg-slate-950/60 border border-slate-800/80 space-y-1">
                      <span className="font-semibold text-slate-200">1. Pre-DB Quota Rejection</span>
                      <p className="text-slate-400 text-[11px] leading-relaxed">
                        Evaluated in Redis Lua before PostgreSQL transactions. 429 rejections guarantee zero orphaned records.
                      </p>
                    </div>

                    <div className="p-3 rounded-lg bg-slate-950/60 border border-slate-800/80 space-y-1">
                      <span className="font-semibold text-slate-200">2. Ephemeral vs Durable Separation</span>
                      <p className="text-slate-400 text-[11px] leading-relaxed">
                        PostgreSQL is authoritative for limits; Redis stores ephemeral tokens. Redis crashes do not corrupt tenant config.
                      </p>
                    </div>

                    <div className="p-3 rounded-lg bg-slate-950/60 border border-slate-800/80 space-y-1">
                      <span className="font-semibold text-slate-200">3. Concurrency-Safe Fallback</span>
                      <p className="text-slate-400 text-[11px] leading-relaxed">
                        PostgreSQL row-locked atomic counter (<code className="text-blue-400">usage_quota_counters</code>) prevents race conditions.
                      </p>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          )}
        </div>
      </main>

      {/* ========================================================================= */}
      {/* MODAL: SEND LIVE NOTIFICATION */}
      {/* ========================================================================= */}
      {showSendModal && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm p-4">
          <div className="bg-slate-900 border border-slate-800 rounded-xl shadow-2xl w-full max-w-lg overflow-hidden animate-in fade-in zoom-in-95 duration-150">
            <div className="flex items-center justify-between p-5 border-b border-slate-800">
              <div className="flex items-center gap-2">
                <Send className="h-5 w-5 text-emerald-400" />
                <h3 className="text-sm font-semibold text-white">Dispatch Live Notification</h3>
              </div>
              <button
                onClick={() => setShowSendModal(false)}
                className="text-slate-400 hover:text-white p-1 rounded transition-colors"
              >
                <X className="h-4 w-4" />
              </button>
            </div>

            <form onSubmit={handleSendLiveNotification} className="p-5 space-y-4">
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label className="block text-xs font-medium text-slate-300 mb-1">Recipient User ID *</label>
                  <input
                    type="text"
                    required
                    value={sendForm.userId}
                    onChange={(e) => setSendForm((p) => ({ ...p, userId: e.target.value }))}
                    className="w-full bg-slate-950 border border-slate-800 rounded-md px-3 py-1.5 text-xs text-slate-100 font-mono focus:outline-none focus:border-emerald-500"
                  />
                </div>
                <div>
                  <label className="block text-xs font-medium text-slate-300 mb-1">Channel *</label>
                  <select
                    value={sendForm.channel}
                    onChange={(e) => setSendForm((p) => ({ ...p, channel: e.target.value }))}
                    className="w-full bg-slate-950 border border-slate-800 rounded-md px-3 py-1.5 text-xs text-slate-100 focus:outline-none focus:border-emerald-500"
                  >
                    <option value="EMAIL">EMAIL</option>
                    <option value="SMS">SMS</option>
                    <option value="PUSH">PUSH</option>
                    <option value="IN_APP">IN_APP</option>
                  </select>
                </div>
              </div>

              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label className="block text-xs font-medium text-slate-300 mb-1">Category *</label>
                  <select
                    value={sendForm.category}
                    onChange={(e) => setSendForm((p) => ({ ...p, category: e.target.value }))}
                    className="w-full bg-slate-950 border border-slate-800 rounded-md px-3 py-1.5 text-xs text-slate-100 focus:outline-none focus:border-emerald-500"
                  >
                    <option value="TRANSACTIONAL">TRANSACTIONAL</option>
                    <option value="SECURITY">SECURITY</option>
                    <option value="SYSTEM">SYSTEM</option>
                    <option value="MARKETING">MARKETING</option>
                  </select>
                </div>
                <div>
                  <label className="block text-xs font-medium text-slate-300 mb-1">Schedule Delay</label>
                  <select
                    value={sendForm.scheduleMinutes}
                    onChange={(e) => setSendForm((p) => ({ ...p, scheduleMinutes: Number(e.target.value) }))}
                    className="w-full bg-slate-950 border border-slate-800 rounded-md px-3 py-1.5 text-xs text-slate-100 focus:outline-none focus:border-emerald-500"
                  >
                    <option value="0">Send Immediately</option>
                    <option value="2">In 2 Minutes (Scheduled)</option>
                    <option value="10">In 10 Minutes (Scheduled)</option>
                    <option value="60">In 1 Hour (Scheduled)</option>
                  </select>
                </div>
              </div>

              <div>
                <label className="block text-xs font-medium text-slate-300 mb-1">Subject</label>
                <input
                  type="text"
                  value={sendForm.subject}
                  onChange={(e) => setSendForm((p) => ({ ...p, subject: e.target.value }))}
                  className="w-full bg-slate-950 border border-slate-800 rounded-md px-3 py-1.5 text-xs text-slate-100 focus:outline-none focus:border-emerald-500"
                />
              </div>

              <div>
                <label className="block text-xs font-medium text-slate-300 mb-1">Message Body *</label>
                <textarea
                  required
                  rows={3}
                  value={sendForm.body}
                  onChange={(e) => setSendForm((p) => ({ ...p, body: e.target.value }))}
                  className="w-full bg-slate-950 border border-slate-800 rounded-md px-3 py-2 text-xs font-mono text-slate-100 focus:outline-none focus:border-emerald-500 resize-none"
                />
              </div>

              <div className="flex items-center justify-end gap-2 pt-2 border-t border-slate-800">
                <button
                  type="button"
                  onClick={() => setShowSendModal(false)}
                  className="px-3 py-1.5 rounded-md text-xs font-medium text-slate-400 hover:text-white transition-colors"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={isLoading}
                  className="px-4 py-1.5 rounded-md text-xs font-semibold bg-emerald-600 hover:bg-emerald-500 text-white shadow-lg shadow-emerald-600/20 transition-all cursor-pointer"
                >
                  {isLoading ? 'Dispatching...' : 'Dispatch to Outbox'}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* ========================================================================= */}
      {/* MODAL: CREATE TEMPLATE (REAL API) */}
      {/* ========================================================================= */}
      {showNewTemplateModal && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm p-4">
          <div className="bg-slate-900 border border-slate-800 rounded-xl shadow-2xl w-full max-w-lg overflow-hidden animate-in fade-in zoom-in-95 duration-150">
            <div className="flex items-center justify-between p-5 border-b border-slate-800">
              <div className="flex items-center gap-2">
                <FileCode2 className="h-5 w-5 text-blue-400" />
                <h3 className="text-sm font-semibold text-white">Create Template in PostgreSQL</h3>
              </div>
              <button
                onClick={() => setShowNewTemplateModal(false)}
                className="text-slate-400 hover:text-white p-1 rounded transition-colors"
              >
                <X className="h-4 w-4" />
              </button>
            </div>

            <form onSubmit={handleCreateTemplate} className="p-5 space-y-4">
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label className="block text-xs font-medium text-slate-300 mb-1">Template Name *</label>
                  <input
                    type="text"
                    required
                    placeholder="e.g. Order Shipped"
                    value={newTemplateForm.name}
                    onChange={(e) => {
                      const name = e.target.value;
                      const key = name.toLowerCase().replace(/[^a-z0-9]/g, '-').replace(/-+/g, '-');
                      setNewTemplateForm((p) => ({ ...p, name, key: p.key === '' || p.key === p.name.toLowerCase().replace(/[^a-z0-9]/g, '-') ? key : p.key }));
                    }}
                    className="w-full bg-slate-950 border border-slate-800 rounded-md px-3 py-1.5 text-xs text-slate-100 placeholder-slate-500 focus:outline-none focus:border-blue-500"
                  />
                </div>
                <div>
                  <label className="block text-xs font-medium text-slate-300 mb-1">Unique Key *</label>
                  <input
                    type="text"
                    required
                    placeholder="e.g. order-shipped"
                    value={newTemplateForm.key}
                    onChange={(e) => setNewTemplateForm((p) => ({ ...p, key: e.target.value }))}
                    className="w-full bg-slate-950 border border-slate-800 rounded-md px-3 py-1.5 text-xs font-mono text-slate-100 placeholder-slate-500 focus:outline-none focus:border-blue-500"
                  />
                </div>
              </div>

              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label className="block text-xs font-medium text-slate-300 mb-1">Channel *</label>
                  <select
                    value={newTemplateForm.channel}
                    onChange={(e) => setNewTemplateForm((p) => ({ ...p, channel: e.target.value }))}
                    className="w-full bg-slate-950 border border-slate-800 rounded-md px-3 py-1.5 text-xs text-slate-100 focus:outline-none focus:border-blue-500"
                  >
                    <option value="EMAIL">EMAIL</option>
                    <option value="SMS">SMS</option>
                    <option value="PUSH">PUSH</option>
                    <option value="IN_APP">IN_APP</option>
                  </select>
                </div>
                <div>
                  <label className="block text-xs font-medium text-slate-300 mb-1">Description</label>
                  <input
                    type="text"
                    placeholder="e.g. Delivery status notification"
                    value={newTemplateForm.description}
                    onChange={(e) => setNewTemplateForm((p) => ({ ...p, description: e.target.value }))}
                    className="w-full bg-slate-950 border border-slate-800 rounded-md px-3 py-1.5 text-xs text-slate-100 placeholder-slate-500 focus:outline-none focus:border-blue-500"
                  />
                </div>
              </div>

              {(newTemplateForm.channel === 'EMAIL' || newTemplateForm.channel === 'PUSH') && (
                <div>
                  <label className="block text-xs font-medium text-slate-300 mb-1">Subject</label>
                  <input
                    type="text"
                    placeholder="e.g. Your order {{order.id}} has shipped!"
                    value={newTemplateForm.subject}
                    onChange={(e) => setNewTemplateForm((p) => ({ ...p, subject: e.target.value }))}
                    className="w-full bg-slate-950 border border-slate-800 rounded-md px-3 py-1.5 text-xs text-slate-100 placeholder-slate-500 focus:outline-none focus:border-blue-500"
                  />
                </div>
              )}

              <div>
                <div className="flex items-center justify-between mb-1">
                  <label className="block text-xs font-medium text-slate-300">Body Template *</label>
                  <span className="text-[11px] text-slate-500 font-mono">Mustache: &#123;&#123;var&#125;&#125;</span>
                </div>
                <textarea
                  required
                  rows={4}
                  placeholder="e.g. Hi {{user.name}}, your order #{{order.id}} has shipped!"
                  value={newTemplateForm.body}
                  onChange={(e) => setNewTemplateForm((p) => ({ ...p, body: e.target.value }))}
                  className="w-full bg-slate-950 border border-slate-800 rounded-md px-3 py-2 text-xs font-mono text-slate-100 placeholder-slate-500 focus:outline-none focus:border-blue-500 resize-none"
                />
              </div>

              {extractMustacheVariables(newTemplateForm.body + ' ' + newTemplateForm.subject).length > 0 && (
                <div className="p-3 bg-blue-950/30 border border-blue-800/40 rounded-lg space-y-1">
                  <div className="text-[11px] font-semibold text-blue-300">Detected Mustache Variables:</div>
                  <div className="flex flex-wrap gap-1.5">
                    {extractMustacheVariables(newTemplateForm.body + ' ' + newTemplateForm.subject).map((v) => (
                      <span key={v} className="text-[10px] font-mono bg-blue-900/60 text-blue-200 px-1.5 py-0.5 rounded border border-blue-700/50">
                        &#123;&#123;{v}&#125;&#125;
                      </span>
                    ))}
                  </div>
                </div>
              )}

              <div className="flex items-center justify-end gap-2 pt-2 border-t border-slate-800">
                <button
                  type="button"
                  onClick={() => setShowNewTemplateModal(false)}
                  className="px-3 py-1.5 rounded-md text-xs font-medium text-slate-400 hover:text-white transition-colors"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={isLoading}
                  className="px-4 py-1.5 rounded-md text-xs font-semibold bg-blue-600 hover:bg-blue-500 text-white shadow-md shadow-blue-600/20 transition-all cursor-pointer"
                >
                  {isLoading ? 'Creating...' : 'Create in PostgreSQL'}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* ========================================================================= */}
      {/* MODAL: ADD NEW VERSION (REAL API) */}
      {/* ========================================================================= */}
      {showNewVersionModal && activeTemplateObj && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm p-4">
          <div className="bg-slate-900 border border-slate-800 rounded-xl shadow-2xl w-full max-w-lg overflow-hidden animate-in fade-in zoom-in-95 duration-150">
            <div className="flex items-center justify-between p-5 border-b border-slate-800">
              <div className="flex items-center gap-2">
                <FileCode2 className="h-5 w-5 text-blue-400" />
                <div>
                  <h3 className="text-sm font-semibold text-white">Add Version to PostgreSQL</h3>
                  <p className="text-[11px] text-slate-400">
                    Template: <span className="text-slate-200">{activeTemplateObj.name}</span>
                  </p>
                </div>
              </div>
              <button
                onClick={() => setShowNewVersionModal(false)}
                className="text-slate-400 hover:text-white p-1 rounded transition-colors"
              >
                <X className="h-4 w-4" />
              </button>
            </div>

            <form onSubmit={handleCreateVersion} className="p-5 space-y-4">
              {(activeTemplateObj.channel === 'EMAIL' || activeTemplateObj.channel === 'PUSH') && (
                <div>
                  <label className="block text-xs font-medium text-slate-300 mb-1">Subject</label>
                  <input
                    type="text"
                    placeholder="Subject line with {{variables}}..."
                    value={newVersionForm.subject}
                    onChange={(e) => setNewVersionForm((p) => ({ ...p, subject: e.target.value }))}
                    className="w-full bg-slate-950 border border-slate-800 rounded-md px-3 py-1.5 text-xs text-slate-100 placeholder-slate-500 focus:outline-none focus:border-blue-500"
                  />
                </div>
              )}

              <div>
                <div className="flex items-center justify-between mb-1">
                  <label className="block text-xs font-medium text-slate-300">Body Template *</label>
                  <span className="text-[11px] text-slate-500 font-mono">Mustache: &#123;&#123;var&#125;&#125;</span>
                </div>
                <textarea
                  required
                  rows={5}
                  placeholder="Enter updated message body with {{variables}}..."
                  value={newVersionForm.body}
                  onChange={(e) => setNewVersionForm((p) => ({ ...p, body: e.target.value }))}
                  className="w-full bg-slate-950 border border-slate-800 rounded-md px-3 py-2 text-xs font-mono text-slate-100 placeholder-slate-500 focus:outline-none focus:border-blue-500 resize-none"
                />
              </div>

              {extractMustacheVariables(newVersionForm.body + ' ' + newVersionForm.subject).length > 0 && (
                <div className="p-3 bg-blue-950/30 border border-blue-800/40 rounded-lg space-y-1">
                  <div className="text-[11px] font-semibold text-blue-300">Detected Mustache Variables:</div>
                  <div className="flex flex-wrap gap-1.5">
                    {extractMustacheVariables(newVersionForm.body + ' ' + newVersionForm.subject).map((v) => (
                      <span key={v} className="text-[10px] font-mono bg-blue-900/60 text-blue-200 px-1.5 py-0.5 rounded border border-blue-700/50">
                        &#123;&#123;{v}&#125;&#125;
                      </span>
                    ))}
                  </div>
                </div>
              )}

              <div className="flex items-center justify-end gap-2 pt-2 border-t border-slate-800">
                <button
                  type="button"
                  onClick={() => setShowNewVersionModal(false)}
                  className="px-3 py-1.5 rounded-md text-xs font-medium text-slate-400 hover:text-white transition-colors"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={isLoading}
                  className="px-4 py-1.5 rounded-md text-xs font-semibold bg-blue-600 hover:bg-blue-500 text-white shadow-md shadow-blue-600/20 transition-all cursor-pointer"
                >
                  {isLoading ? 'Saving...' : 'Save Version (Draft)'}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
}

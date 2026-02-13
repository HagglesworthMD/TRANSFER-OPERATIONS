/* Live Insights Panel */

const LiveInsights = {
    update(data) {
        if (!data || !data.summary) return;

        const summary = data.summary;
        const nextName = summary.next_staff || '—';

        // Panel title
        this._set('insight-panel-title', `Next Up: ${nextName}`);

        // Find next staff member's KPIs
        const kpi = (data.staff_kpis || []).find(s => s.name === nextName);

        // Per-staff metrics
        this._set('insight-active', kpi ? kpi.active : '—');
        this._set('insight-assigned', kpi ? kpi.assigned : '—');
        this._set('insight-completed', kpi ? kpi.completed : '—');
        this._set('insight-median', kpi?.median_human || '—');
    },

    _set(id, value) {
        const el = document.getElementById(id);
        if (!el) return;
        el.textContent = String(value);
    },
};

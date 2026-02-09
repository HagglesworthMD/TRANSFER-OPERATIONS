/* Live Insights Panel */

const LiveInsights = {
    update(data) {
        if (!data || !data.summary) return;

        const summary = data.summary;

        // Active tickets
        this._set('insight-active', summary.active_count || 0);

        // Completion rate (completions / processed * 100)
        const processed = summary.processed_today || 0;
        const completions = summary.completions_today || 0;
        let rate = '—';
        if (processed > 0) {
            const pct = Math.round((completions / processed) * 100);
            rate = `${pct}%`;
        } else if (completions > 0) {
            rate = `${completions} done`;
        }
        this._set('insight-completion-rate', rate);

        // Average response time
        this._set('insight-avg-response', summary.avg_time_human || 'N/A');

        // Next staff member
        this._set('insight-next-staff', summary.next_staff || '—');

        // Total processed (lifetime)
        this._set('insight-total', summary.total_processed || '—');

        // Roster position
        this._set('insight-roster', summary.roster_index != null ? summary.roster_index : '—');
    },

    _set(id, value) {
        const el = document.getElementById(id);
        if (!el) return;
        el.textContent = String(value);
    },
};

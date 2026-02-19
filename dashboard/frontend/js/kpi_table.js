/* Sortable per-staff KPI table */

const KPITable = {
    _data: [],
    _sortKey: 'assigned',
    _sortDir: 'desc',
    _assignedMode: 'all',

    init() {
        const headers = document.querySelectorAll('#kpi-table thead th.sortable');
        headers.forEach(th => {
            th.addEventListener('click', () => {
                const key = th.dataset.sort;
                if (this._sortKey === key) {
                    this._sortDir = this._sortDir === 'asc' ? 'desc' : 'asc';
                } else {
                    this._sortKey = key;
                    this._sortDir = key === 'name' ? 'asc' : 'desc';
                }
                this._updateSortIndicators();
                this._render();
            });
        });

        // Assigned toggle: switch between all-time and in-range
        const toggleBtn = document.getElementById('assigned-toggle');
        if (toggleBtn) {
            toggleBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                this._assignedMode = this._assignedMode === 'all' ? 'range' : 'all';
                toggleBtn.textContent = this._assignedMode === 'all' ? 'All' : 'Range';
                this._render();
                document.dispatchEvent(new CustomEvent('assigned-mode-change', {
                    detail: { mode: this._assignedMode }
                }));
            });
        }

        // Delegated click handler for staff name CSV export
        const tbody = document.getElementById('kpi-tbody');
        if (tbody) {
            tbody.addEventListener('click', (e) => {
                const cell = e.target.closest('.cell-name-link');
                if (!cell) return;
                const name = cell.dataset.name;
                if (!name) return;
                const dsEl = document.getElementById('date-start');
                const deEl = document.getElementById('date-end');
                const dateStart = dsEl ? dsEl.value : '';
                const dateEnd = deEl ? deEl.value : '';
                const url = `/api/staff-export?name=${encodeURIComponent(name)}&date_start=${encodeURIComponent(dateStart)}&date_end=${encodeURIComponent(dateEnd)}`;
                window.open(url, '_blank');
            });
        }
    },

    update(staffKpis) {
        if (!staffKpis) return;
        this._data = staffKpis;
        this._render();
    },

    _updateSortIndicators() {
        document.querySelectorAll('#kpi-table thead th.sortable').forEach(th => {
            th.classList.remove('sort-asc', 'sort-desc');
            if (th.dataset.sort === this._sortKey) {
                th.classList.add(this._sortDir === 'asc' ? 'sort-asc' : 'sort-desc');
            }
        });
    },

    _render() {
        const tbody = document.getElementById('kpi-tbody');
        if (!tbody) return;

        const effectiveSortKey = (this._sortKey === 'assigned' && this._assignedMode === 'range')
            ? 'assigned_in_range' : this._sortKey;
        const sorted = [...this._data].sort((a, b) => {
            let av = a[effectiveSortKey];
            let bv = b[effectiveSortKey];
            if (av == null) av = -Infinity;
            if (bv == null) bv = -Infinity;
            if (typeof av === 'string') av = av.toLowerCase();
            if (typeof bv === 'string') bv = bv.toLowerCase();
            if (av < bv) return this._sortDir === 'asc' ? -1 : 1;
            if (av > bv) return this._sortDir === 'asc' ? 1 : -1;
            return 0;
        });

        const P90_WARN = 15.0;
        const ACTIVE_AMBER = 3;

        tbody.innerHTML = sorted.map(s => {
            let rowClass = '';
            if (s.active === 0) rowClass = 'row-green';
            else if (s.active > ACTIVE_AMBER) rowClass = 'row-amber';

            const p90Class = (s.p90_min != null && s.p90_min > P90_WARN) ? 'p90-warning' : '';
            const lcIcon = s.low_confidence
                ? '<span class="low-confidence-icon" title="Low sample size (&lt;10 completions)">!</span>'
                : '';

            return `<tr class="${rowClass}">
                <td class="cell-name cell-name-link" data-name="${this._esc(s.name)}">${this._esc(s.name)}</td>
                <td>${this._assignedMode === 'all' ? s.assigned : s.assigned_in_range}</td>
                <td>${s.completed}</td>
                <td>${s.active}</td>
                <td>${s.median_human || '—'}${lcIcon}</td>
                <td class="${p90Class}">${s.p90_human || '—'}${lcIcon}</td>
            </tr>`;
        }).join('');

        // Totals footer row
        const tfoot = document.getElementById('kpi-tfoot');
        if (tfoot) {
            let totalAssigned = 0, totalCompleted = 0, totalActive = 0;
            for (const s of this._data) {
                totalAssigned += this._assignedMode === 'all' ? s.assigned : s.assigned_in_range;
                totalCompleted += s.completed;
                totalActive += s.active;
            }
            tfoot.innerHTML = `<tr class="kpi-totals-row">
                <td>Total</td>
                <td>${totalAssigned}</td>
                <td>${totalCompleted}</td>
                <td>${totalActive}</td>
                <td></td>
                <td></td>
            </tr>`;
        }
    },

    _esc(str) {
        const d = document.createElement('div');
        d.textContent = str || '';
        return d.innerHTML;
    },
};

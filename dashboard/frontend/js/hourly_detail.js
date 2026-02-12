/* Hourly detail table — event breakdown by source */

const HourlyDetail = {
    _currentData: null,
    _selectedHour: null,
    _sourceFilter: '',
    _sortKey: 'time',
    _sortDir: 'desc',

    init() {
        const sourceSelect = document.getElementById('hourly-source-filter');
        const clearBtn = document.getElementById('hourly-clear-filter');

        if (sourceSelect) {
            sourceSelect.addEventListener('change', (e) => {
                this._sourceFilter = e.target.value;
                this._render();
            });
        }

        if (clearBtn) {
            clearBtn.addEventListener('click', () => {
                this._selectedHour = null;
                this._sourceFilter = '';
                if (sourceSelect) sourceSelect.value = '';
                clearBtn.style.display = 'none';
                this._updateFilterLabel();
                this._render();
            });
        }

        // Listen for bar click from chart
        document.addEventListener('hourly-bar-click', (e) => {
            this._selectedHour = e.detail.hour;
            this._updateFilterLabel();
            this._render();

            // Auto-scroll to detail section
            const section = document.getElementById('hourly-detail-section');
            if (section) section.scrollIntoView({ behavior: 'smooth', block: 'start' });
        });

        // Column sorting
        const headers = document.querySelectorAll('#hourly-detail-table thead th.sortable');
        headers.forEach(th => {
            th.addEventListener('click', () => {
                const key = th.dataset.sort;
                if (this._sortKey === key) {
                    this._sortDir = this._sortDir === 'asc' ? 'desc' : 'asc';
                } else {
                    this._sortKey = key;
                    this._sortDir = (key === 'subject' || key === 'staff') ? 'asc' : 'desc';
                }
                this._updateSortIndicators();
                this._render();
            });
        });
    },

    update(detail) {
        if (!detail) return;
        this._currentData = detail;
        this._updateSourceDropdown(detail.all_sources || []);
        this._render();
    },

    _updateSourceDropdown(sources) {
        const select = document.getElementById('hourly-source-filter');
        if (!select) return;

        const current = select.value;
        const options = sources.map(s =>
            `<option value="${this._esc(s)}">${this._esc(s)}</option>`
        ).join('');

        select.innerHTML = '<option value="">All Sources</option>' + options;

        if (current && sources.includes(current)) {
            select.value = current;
        }
    },

    _updateFilterLabel() {
        const label = document.getElementById('hourly-filter-label');
        const hourSpan = document.getElementById('hourly-filter-hour');
        const clearBtn = document.getElementById('hourly-clear-filter');

        if (this._selectedHour) {
            if (label) label.style.display = 'inline';
            if (hourSpan) hourSpan.textContent = this._selectedHour;
            if (clearBtn) clearBtn.style.display = 'inline-block';
        } else {
            if (label) label.style.display = 'none';
            if (clearBtn && !this._sourceFilter) clearBtn.style.display = 'none';
        }
    },

    _updateSortIndicators() {
        document.querySelectorAll('#hourly-detail-table thead th.sortable').forEach(th => {
            th.classList.remove('sort-asc', 'sort-desc');
            if (th.dataset.sort === this._sortKey) {
                th.classList.add(this._sortDir === 'asc' ? 'sort-asc' : 'sort-desc');
            }
        });
    },

    _sortValue(item, key) {
        switch (key) {
            case 'time':    return item.time || '';
            case 'source':  return (item.source || '').toLowerCase();
            case 'type':    return item.type || '';
            case 'subject': return (item.subject || '').toLowerCase();
            case 'staff':   return (item.staff || '').toLowerCase();
            case 'risk': {
                const rank = { critical: 4, urgent: 3, high: 3, normal: 1, low: 0 };
                return rank[(item.risk || '').toLowerCase()] || 0;
            }
            default: return '';
        }
    },

    _render() {
        const tbody = document.getElementById('hourly-detail-tbody');
        const badge = document.getElementById('hourly-detail-count');
        const clearBtn = document.getElementById('hourly-clear-filter');
        if (!tbody || !this._currentData) return;

        const hours = this._currentData.hours || {};

        // Collect events based on filters
        let events = [];
        if (this._selectedHour) {
            const bucket = hours[this._selectedHour];
            if (bucket) events = bucket.events || [];
        } else {
            // All hours
            Object.values(hours).forEach(bucket => {
                if (bucket.events) events = events.concat(bucket.events);
            });
        }

        // Source filter
        if (this._sourceFilter) {
            events = events.filter(e => e.source === this._sourceFilter);
        }

        // Show clear button if any filter is active
        if (clearBtn) {
            clearBtn.style.display = (this._selectedHour || this._sourceFilter) ? 'inline-block' : 'none';
        }

        // Sort
        events = [...events].sort((a, b) => {
            let av = this._sortValue(a, this._sortKey);
            let bv = this._sortValue(b, this._sortKey);
            if (av < bv) return this._sortDir === 'asc' ? -1 : 1;
            if (av > bv) return this._sortDir === 'asc' ? 1 : -1;
            return 0;
        });

        if (badge) badge.textContent = events.length;

        const rows = events.map(item => {
            const sourceBadge = `<span class="source-badge" data-source="${this._esc(item.source.toLowerCase())}">${this._esc(item.source)}</span>`;
            const typeBadge = `<span class="type-badge ${(item.type || '').toLowerCase()}">${this._esc(item.type)}</span>`;
            const riskBadge = item.risk
                ? `<span class="risk-badge ${item.risk}">${this._esc(item.risk)}</span>`
                : '';

            return `<tr>
                <td>${this._esc(item.time)}</td>
                <td>${sourceBadge}</td>
                <td>${typeBadge}</td>
                <td title="${this._esc(item.subject)}">${this._esc(this._truncate(item.subject, 55))}</td>
                <td>${this._esc(item.staff)}</td>
                <td>${riskBadge}</td>
            </tr>`;
        });

        tbody.innerHTML = rows.join('');
    },

    _truncate(str, len) {
        if (!str) return '';
        return str.length > len ? str.substring(0, len) + '...' : str;
    },

    _esc(str) {
        const d = document.createElement('div');
        d.textContent = str || '';
        return d.innerHTML;
    },
};

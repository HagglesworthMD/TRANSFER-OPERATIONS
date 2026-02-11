/* Recent activity table — live scrolling feed */

const ActivityFeed = {
    _prevKeys: new Set(),
    _currentFeed: [],
    _currentFilter: '',
    _sortKey: 'time',
    _sortDir: 'desc',

    init() {
        const filterSelect = document.getElementById('staff-filter');
        const clearBtn = document.getElementById('clear-filter');

        if (filterSelect) {
            filterSelect.addEventListener('change', (e) => {
                this._currentFilter = e.target.value;
                clearBtn.style.display = this._currentFilter ? 'inline-block' : 'none';
                // Re-fetch from backend with staff filter so we get all their events
                App.refresh();
            });
        }

        if (clearBtn) {
            clearBtn.addEventListener('click', () => {
                this._currentFilter = '';
                filterSelect.value = '';
                clearBtn.style.display = 'none';
                App.refresh();
            });
        }

        // Column sorting
        const headers = document.querySelectorAll('#activity-feed-table thead th.sortable');
        headers.forEach(th => {
            th.addEventListener('click', () => {
                const key = th.dataset.sort;
                if (this._sortKey === key) {
                    this._sortDir = this._sortDir === 'asc' ? 'desc' : 'asc';
                } else {
                    this._sortKey = key;
                    // Default to ascending for text columns, descending for others
                    this._sortDir = (key === 'subject' || key === 'assigned_to') ? 'asc' : 'desc';
                }
                this._updateSortIndicators();
                this._applyFilter();
            });
        });
    },

    /** Full unfiltered feed — used to build the staff dropdown. */
    _allStaffNames: new Set(),

    update(feed) {
        if (!feed) return;

        this._currentFeed = feed;
        this._updateStaffFilter(feed);
        this._applyFilter();
    },

    _updateStaffFilter(feed) {
        const filterSelect = document.getElementById('staff-filter');
        if (!filterSelect) return;

        // Accumulate staff names across refreshes so the dropdown stays
        // complete even when a single-staff filter is active.
        feed.forEach(item => {
            if (item.assigned_to && item.assigned_to !== 'completed' && item.assigned_to !== 'error') {
                this._allStaffNames.add(item.assigned_to);
            }
        });

        // Preserve current selection
        const currentValue = filterSelect.value;

        // Update options from the accumulated set
        const staffOptions = Array.from(this._allStaffNames).sort().map(staff =>
            `<option value="${this._esc(staff)}">${this._esc(staff)}</option>`
        ).join('');

        filterSelect.innerHTML = '<option value="">All Staff</option>' + staffOptions;

        // Restore selection if still valid
        if (currentValue && this._allStaffNames.has(currentValue)) {
            filterSelect.value = currentValue;
        }
    },

    _updateSortIndicators() {
        document.querySelectorAll('#activity-feed-table thead th.sortable').forEach(th => {
            th.classList.remove('sort-asc', 'sort-desc');
            if (th.dataset.sort === this._sortKey) {
                th.classList.add(this._sortDir === 'asc' ? 'sort-asc' : 'sort-desc');
            }
        });
    },

    /** Map data-sort key to the comparable value on each feed item. */
    _sortValue(item, key) {
        switch (key) {
            case 'time':        return item.time || '';
            case 'type':        return item.type || '';
            case 'subject':     return (item.subject || '').toLowerCase();
            case 'assigned_to': return (item.assigned_to || '').toLowerCase();
            case 'duration_sec': return item.duration_sec ?? -1;
            case 'risk_level': {
                // Order: critical > high > medium > low > (none)
                const rank = { critical: 4, high: 3, medium: 2, low: 1 };
                return rank[(item.risk_level || '').toLowerCase()] || 0;
            }
            default: return '';
        }
    },

    _applyFilter() {
        const tbody = document.getElementById('feed-tbody');
        const badge = document.getElementById('feed-count');
        if (!tbody) return;

        let filteredFeed = this._currentFeed;

        if (this._currentFilter) {
            filteredFeed = this._currentFeed.filter(item =>
                item.assigned_to === this._currentFilter
            );
        }

        // Sort
        filteredFeed = [...filteredFeed].sort((a, b) => {
            let av = this._sortValue(a, this._sortKey);
            let bv = this._sortValue(b, this._sortKey);
            if (av < bv) return this._sortDir === 'asc' ? -1 : 1;
            if (av > bv) return this._sortDir === 'asc' ? 1 : -1;
            return 0;
        });

        if (badge) badge.textContent = filteredFeed.length;

        const newKeys = new Set();
        const rows = filteredFeed.map(item => {
            const key = `${item.date}-${item.time}-${item.type}-${item.subject}`;
            newKeys.add(key);
            const isNew = !this._prevKeys.has(key) && this._prevKeys.size > 0;

            const typeBadge = `<span class="type-badge ${item.type.toLowerCase()}">${item.type}</span>`;
            const riskBadge = item.risk_level
                ? `<span class="risk-badge ${item.risk_level}">${item.risk_level}</span>`
                : '';

            const subjectHtml = this._linkifyRefs(this._truncate(item.subject, 55));

            return `<tr class="${isNew ? 'new-row' : ''}">
                <td>${this._esc(item.time)}</td>
                <td>${typeBadge}</td>
                <td title="${this._esc(item.subject)}">${subjectHtml}</td>
                <td>${this._esc(item.assigned_to)}</td>
                <td>${this._esc(item.duration_human)}</td>
                <td>${riskBadge}</td>
            </tr>`;
        });

        tbody.innerHTML = rows.join('');
        this._prevKeys = newKeys;

        // Wire up click-to-copy on ref badges
        tbody.querySelectorAll('.ref-badge').forEach(el => {
            el.addEventListener('click', (e) => {
                e.stopPropagation();
                const ref = el.dataset.ref;
                navigator.clipboard.writeText(ref).then(() => {
                    el.classList.add('copied');
                    el.setAttribute('title', 'Copied!');
                    setTimeout(() => {
                        el.classList.remove('copied');
                        el.setAttribute('title', `Click to copy ${ref}`);
                    }, 1500);
                });
            });
        });
    },

    _linkifyRefs(str) {
        if (!str) return '';
        // Match [SAMI-XXXXXX] or REF:xxxxxxxxxxxx patterns
        return this._esc(str).replace(
            /\[SAMI-([A-Fa-f0-9]+)\]/g,
            (match, code) => `<span class="ref-badge" data-ref="SAMI-${code}" title="Click to copy SAMI-${code}">SAMI-${code}</span>`
        ).replace(
            /REF:([a-f0-9]+)/g,
            (match, code) => `<span class="ref-badge" data-ref="${code}" title="Click to copy ${code}">${code.substring(0, 8)}...</span>`
        );
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

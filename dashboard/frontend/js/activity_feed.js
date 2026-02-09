/* Recent activity table — live scrolling feed */

const ActivityFeed = {
    _prevKeys: new Set(),
    _currentFeed: [],
    _currentFilter: '',

    init() {
        const filterSelect = document.getElementById('staff-filter');
        const clearBtn = document.getElementById('clear-filter');

        if (filterSelect) {
            filterSelect.addEventListener('change', (e) => {
                this._currentFilter = e.target.value;
                this._applyFilter();
                clearBtn.style.display = this._currentFilter ? 'inline-block' : 'none';
            });
        }

        if (clearBtn) {
            clearBtn.addEventListener('click', () => {
                this._currentFilter = '';
                filterSelect.value = '';
                this._applyFilter();
                clearBtn.style.display = 'none';
            });
        }
    },

    update(feed) {
        if (!feed) return;

        this._currentFeed = feed;
        this._updateStaffFilter(feed);
        this._applyFilter();
    },

    _updateStaffFilter(feed) {
        const filterSelect = document.getElementById('staff-filter');
        if (!filterSelect) return;

        // Get unique staff members
        const staffSet = new Set();
        feed.forEach(item => {
            if (item.assigned_to && item.assigned_to !== 'completed' && item.assigned_to !== 'error') {
                staffSet.add(item.assigned_to);
            }
        });

        // Preserve current selection
        const currentValue = filterSelect.value;

        // Update options
        const staffOptions = Array.from(staffSet).sort().map(staff =>
            `<option value="${this._esc(staff)}">${this._esc(staff)}</option>`
        ).join('');

        filterSelect.innerHTML = '<option value="">All Staff</option>' + staffOptions;

        // Restore selection if still valid
        if (currentValue && staffSet.has(currentValue)) {
            filterSelect.value = currentValue;
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

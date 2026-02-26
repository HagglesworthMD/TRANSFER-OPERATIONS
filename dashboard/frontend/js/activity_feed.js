/* Recent activity table - live scrolling feed */

const ActivityFeed = {
    _prevKeys: new Set(),
    _currentFeed: [],
    _currentFilter: '',
    _sortKey: 'time',
    _sortDir: 'desc',
    _activeRows: [],
    _activeParams: null,

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

        const showActiveBtn = document.getElementById('show-active-btn');
        const samiSearchInput = document.getElementById('sami-search-input');
        const samiExportBtn = document.getElementById('sami-export-btn');
        const activeCloseBtn = document.getElementById('active-close-btn');
        const activeBackdrop = document.getElementById('active-modal-backdrop');
        const activeDownloadBtn = document.getElementById('active-download-btn');

        if (showActiveBtn) {
            showActiveBtn.addEventListener('click', () => this._openActiveModal());
        }
        const activeCard = document.getElementById('active-card');
        if (activeCard) {
            activeCard.addEventListener('click', () => this._openActiveModal());
        }
        if (activeCloseBtn) {
            activeCloseBtn.addEventListener('click', () => this._closeActiveModal());
        }
        if (activeBackdrop) {
            activeBackdrop.addEventListener('click', () => this._closeActiveModal());
        }
        if (activeDownloadBtn) {
            activeDownloadBtn.addEventListener('click', () => {
                const params = this._activeParams || this._currentDateParams();
                const url = DashboardAPI.getActiveCsvUrl(params);
                window.open(url, '_blank');
            });
        }
        const runSamiExport = () => {
            if (!samiSearchInput) return;
            const samiRef = (samiSearchInput.value || '').trim();
            if (!samiRef) {
                alert('Enter a SAMI code first (e.g. SAMI-ABC123).');
                samiSearchInput.focus();
                return;
            }
            const url = DashboardAPI.getSamiCsvUrl(samiRef);
            window.open(url, '_blank');
        };
        if (samiExportBtn) {
            samiExportBtn.addEventListener('click', runSamiExport);
        }
        if (samiSearchInput) {
            samiSearchInput.addEventListener('keydown', (e) => {
                if (e.key === 'Enter') {
                    e.preventDefault();
                    runSamiExport();
                }
            });
        }

        const reconcileAllBtn = document.getElementById('reconcile-all-btn');
        if (reconcileAllBtn) {
            reconcileAllBtn.addEventListener('click', () => this._reconcileAll());
        }

        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') {
                this._closeActiveModal();
            }
        });

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

    /** Full unfiltered feed - used to build the staff dropdown. */
    _allStaffNames: new Set(),

    update(feed) {
        if (!feed) return;

        this._currentFeed = feed;
        this._updateStaffFilter(feed);
        this._applyFilter();
    },

    _currentDateParams() {
        if (typeof App !== 'undefined' && typeof App._getDateParams === 'function') {
            return App._getDateParams();
        }
        return null;
    },

    async _openActiveModal(staffName) {
        const modal = document.getElementById('active-modal');
        const meta = document.getElementById('active-modal-meta');
        const tbody = document.getElementById('active-modal-tbody');
        const titleEl = document.getElementById('active-modal-title');
        if (!modal || !meta || !tbody) return;

        modal.classList.remove('hidden');
        meta.textContent = 'Loading active tickets...';
        tbody.innerHTML = '<tr><td colspan="9">Loading...</td></tr>';
        if (titleEl) titleEl.textContent = staffName ? `Active Tickets \u2014 ${staffName}` : 'Active Tickets';

        try {
            const params = this._currentDateParams() || {};
            if (staffName) params.staff = staffName;
            this._activeParams = params;
            const data = await DashboardAPI.getActive(params);
            this._activeRows = Array.isArray(data.rows) ? data.rows : [];
            this._renderActiveRows(this._activeRows);
            const label = staffName ? `${staffName} \u2014 ` : '';
            meta.textContent = `${label}${data.count || 0} active tickets (${data.date_start || ''} to ${data.date_end || ''})`;
            this._renderReconciledSection(data.reconciled || []);
        } catch (err) {
            meta.textContent = `Failed to load active tickets: ${err.message}`;
            tbody.innerHTML = '<tr><td colspan="9">Failed to load active tickets.</td></tr>';
        }
    },

    _closeActiveModal() {
        const modal = document.getElementById('active-modal');
        if (modal) {
            modal.classList.add('hidden');
        }
    },

    _renderActiveRows(rows) {
        const tbody = document.getElementById('active-modal-tbody');
        if (!tbody) return;

        if (!rows || rows.length === 0) {
            tbody.innerHTML = '<tr><td colspan="9">No active tickets found for this date range.</td></tr>';
            return;
        }

        const htmlRows = rows.map((item, idx) => {
            const sami = (item.sami_ref || '').trim();
            const samiHtml = sami
                ? `<span class="ref-badge" data-ref="${this._esc(sami)}" title="Click to copy ${this._esc(sami)}">${this._esc(sami)}</span>`
                : '';
            const subjectHtml = this._linkifyRefs(this._truncate(item.subject || '', 90));
            const identity = item.identity || '';
            const hasIdentity = identity.length > 0;
            const reconcileBtn = hasIdentity
                ? `<button class="btn-reconcile" data-idx="${idx}" title="Mark reconciled">Reconcile</button>`
                : '';
            const reassignBtn = sami
                ? `<button class="btn-reassign" data-idx="${idx}" title="Reassign ticket">Reassign</button>`
                : '';

            return `<tr>
                <td>${this._esc(item.date)}</td>
                <td>${this._esc(item.time)}</td>
                <td>${samiHtml}</td>
                <td>${this._esc(item.staff)}</td>
                <td>${this._esc(item.sender)}</td>
                <td>${this._esc(item.domain)}</td>
                <td>${this._esc(item.risk_level)}</td>
                <td title="${this._esc(item.subject)}">${subjectHtml}</td>
                <td>${reconcileBtn} ${reassignBtn}</td>
            </tr>`;
        });

        tbody.innerHTML = htmlRows.join('');
        this._wireRefCopy(tbody);
        this._wireReconcileButtons(tbody, rows);
        this._wireReassignButtons(tbody, rows);
    },

    _wireReconcileButtons(container, rows) {
        container.querySelectorAll('.btn-reconcile').forEach(btn => {
            btn.addEventListener('click', async (e) => {
                e.stopPropagation();
                const idx = parseInt(btn.dataset.idx, 10);
                const item = rows[idx];
                if (!item) return;

                const reason = prompt('Reason for reconciliation (optional):');
                if (reason === null) return; // cancelled

                btn.disabled = true;
                btn.textContent = '...';
                try {
                    await DashboardAPI.reconcile(item.identity, item.staff, reason || '', item.sami_ref || '');
                    await this._openActiveModal();
                } catch (err) {
                    alert('Failed to reconcile: ' + err.message);
                    btn.disabled = false;
                    btn.textContent = 'Reconcile';
                }
            });
        });
    },

    async _reconcileAll() {
        const count = this._activeRows ? this._activeRows.length : 0;
        if (count === 0) {
            alert('No active tickets to reconcile.');
            return;
        }
        const cardEl = document.getElementById('card-active');
        const cardCount = cardEl ? cardEl.textContent.trim() : '?';
        if (!confirm(`Reconcile all active tickets?\nActive count will go from ${cardCount} to 0.`)) {
            return;
        }
        const reason = prompt('Reason (optional):', 'Bulk reconcile — balanced');
        if (reason === null) return; // cancelled

        const btn = document.getElementById('reconcile-all-btn');
        if (btn) { btn.disabled = true; btn.textContent = 'Reconciling...'; }

        try {
            const params = this._activeParams || this._currentDateParams();
            const result = await DashboardAPI.reconcileAll(
                params?.dateStart, params?.dateEnd, params?.staff, reason || 'Bulk reconcile'
            );
            await this._openActiveModal();
            if (typeof App !== 'undefined' && typeof App.refresh === 'function') {
                App.refresh();
            }
        } catch (err) {
            alert('Failed to reconcile all: ' + err.message);
        } finally {
            if (btn) { btn.disabled = false; btn.textContent = 'Reconcile All'; }
        }
    },

    _renderReconciledSection(entries) {
        const section = document.getElementById('reconciled-section');
        const listEl = document.getElementById('reconciled-list');
        const toggleBtn = document.getElementById('toggle-reconciled-btn');
        if (!section || !listEl || !toggleBtn) return;

        if (!entries || entries.length === 0) {
            section.style.display = 'none';
            return;
        }

        section.style.display = '';
        toggleBtn.textContent = `Show Reconciled (${entries.length})`;

        // Remove old listener by cloning
        const newToggle = toggleBtn.cloneNode(true);
        toggleBtn.parentNode.replaceChild(newToggle, toggleBtn);
        newToggle.addEventListener('click', () => {
            listEl.classList.toggle('hidden');
            newToggle.textContent = listEl.classList.contains('hidden')
                ? `Show Reconciled (${entries.length})`
                : `Hide Reconciled (${entries.length})`;
        });

        const rows = entries.map(e => {
            const id = this._esc(e.identity || '');
            const staff = this._esc(e.staff_email || '');
            const sami = this._esc(e.sami_ref || '');
            const reason = this._esc(e.reason || '');
            const ts = this._esc((e.ts || '').substring(0, 19));
            return `<div class="reconciled-item">
                <span class="reconciled-identity">${id}</span>
                <span class="reconciled-staff">${staff}</span>
                ${sami ? `<span class="ref-badge" data-ref="${sami}" title="Click to copy ${sami}">${sami}</span>` : ''}
                ${reason ? `<span class="reconciled-reason">${reason}</span>` : ''}
                <span class="reconciled-ts">${ts}</span>
                <button class="btn-undo" data-identity="${id}">Undo</button>
            </div>`;
        });

        listEl.innerHTML = rows.join('');

        listEl.querySelectorAll('.btn-undo').forEach(btn => {
            btn.addEventListener('click', async (ev) => {
                ev.stopPropagation();
                btn.disabled = true;
                btn.textContent = '...';
                try {
                    await DashboardAPI.removeReconcile(btn.dataset.identity);
                    await this._openActiveModal();
                } catch (err) {
                    alert('Failed to undo: ' + err.message);
                    btn.disabled = false;
                    btn.textContent = 'Undo';
                }
            });
        });
    },

    _wireRefCopy(container) {
        if (!container) return;
        container.querySelectorAll('.ref-badge').forEach(el => {
            el.addEventListener('click', async (e) => {
                e.stopPropagation();
                const ref = (el.dataset.ref || '').trim();
                if (!ref) return;

                const copied = await this._copyToClipboard(ref);
                if (!copied) {
                    console.warn('Clipboard copy failed for ref badge:', ref);
                    return;
                }

                el.classList.add('copied');
                el.setAttribute('title', 'Copied!');
                setTimeout(() => {
                    el.classList.remove('copied');
                    el.setAttribute('title', `Click to copy ${ref}`);
                }, 1500);
            });
        });
    },

    async _copyToClipboard(text) {
        const value = `${text || ''}`;
        if (!value) return false;

        // navigator.clipboard often fails on LAN HTTP; keep a legacy fallback.
        if (navigator.clipboard && typeof navigator.clipboard.writeText === 'function') {
            try {
                await navigator.clipboard.writeText(value);
                return true;
            } catch (err) {
                // Fall through to execCommand fallback.
            }
        }

        return this._copyWithExecCommand(value);
    },

    _copyWithExecCommand(text) {
        const value = `${text || ''}`;
        if (!value) return false;

        const textarea = document.createElement('textarea');
        textarea.value = value;
        textarea.setAttribute('readonly', '');
        textarea.style.position = 'fixed';
        textarea.style.top = '-9999px';
        textarea.style.left = '-9999px';
        textarea.style.opacity = '0';

        document.body.appendChild(textarea);
        textarea.focus();
        textarea.select();
        textarea.setSelectionRange(0, textarea.value.length);

        let copied = false;
        try {
            copied = document.execCommand('copy');
        } catch (err) {
            copied = false;
        }

        document.body.removeChild(textarea);
        return copied;
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

            const samiRef = (item.sami_ref || '').trim();
            let subjectHtml = this._linkifyRefs(this._truncate(item.subject, 55));
            if (samiRef && !(item.subject || '').toUpperCase().includes(samiRef.toUpperCase())) {
                const safeRef = this._esc(samiRef);
                subjectHtml = `<span class="ref-badge" data-ref="${safeRef}" title="Click to copy ${safeRef}">${safeRef}</span> ${subjectHtml}`;
            }

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
        this._wireRefCopy(tbody);
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

    // ── Reassign dialog ──────────────────────────────────────────

    _wireReassignButtons(container, rows) {
        container.querySelectorAll('.btn-reassign').forEach(btn => {
            btn.addEventListener('click', async (e) => {
                e.stopPropagation();
                const idx = parseInt(btn.dataset.idx, 10);
                const item = rows[idx];
                if (!item) return;
                const samiRef = (item.sami_ref || '').trim();
                if (!samiRef) return;
                await this._openReassignDialog(samiRef, (item.staff_email || item.staff || '').trim());
            });
        });
    },

    async _openReassignDialog(samiRef, currentStaff) {
        const dialog = document.getElementById('reassign-dialog');
        const backdrop = document.getElementById('reassign-dialog-backdrop');
        const samiEl = document.getElementById('reassign-sami-ref');
        const modeEl = document.getElementById('reassign-mode');
        const staffField = document.getElementById('reassign-staff-field');
        const staffSelect = document.getElementById('reassign-target-staff');
        const reasonEl = document.getElementById('reassign-reason');
        const noteEl = document.getElementById('reassign-note');
        const confirmBtn = document.getElementById('reassign-confirm-btn');
        const cancelBtn = document.getElementById('reassign-cancel-btn');
        const errorEl = document.getElementById('reassign-error');
        if (!dialog) return;

        // Store context for submit
        this._reassignSamiRef = samiRef;
        this._reassignCurrentStaff = currentStaff;

        // Populate fields
        if (samiEl) samiEl.textContent = samiRef;
        if (modeEl) modeEl.value = 'next_in_rotation';
        if (reasonEl) reasonEl.value = 'leave';
        if (noteEl) noteEl.value = '';
        if (errorEl) { errorEl.textContent = ''; errorEl.classList.add('hidden'); }
        if (staffField) staffField.classList.add('hidden');

        // Populate staff dropdown from API
        if (staffSelect) {
            staffSelect.innerHTML = '<option value="">Loading...</option>';
            try {
                const data = await DashboardAPI.getStaff();
                const allStaff = Array.isArray(data.staff) ? data.staff : (Array.isArray(data) ? data : []);
                const off = new Set((data.off_rotation || []).map(e => e.toLowerCase()));
                const leave = new Set((data.leave || []).map(e => e.toLowerCase()));
                const currentLower = (currentStaff || '').toLowerCase();
                const eligible = allStaff.filter(e => {
                    const lower = e.toLowerCase();
                    return lower !== currentLower && !off.has(lower) && !leave.has(lower);
                });
                if (eligible.length === 0) {
                    staffSelect.innerHTML = '<option value="">No eligible staff</option>';
                } else {
                    staffSelect.innerHTML = eligible.map(e =>
                        `<option value="${this._esc(e)}">${this._esc(e)}</option>`
                    ).join('');
                }
            } catch (err) {
                staffSelect.innerHTML = '<option value="">Failed to load staff</option>';
            }
        }

        // Clone to remove old listeners
        const newMode = modeEl.cloneNode(true);
        modeEl.parentNode.replaceChild(newMode, modeEl);
        const modeHandler = () => {
            if (staffField) {
                staffField.classList.toggle('hidden', newMode.value !== 'target_staff');
            }
        };
        newMode.addEventListener('change', modeHandler);
        modeHandler(); // ensure correct initial visibility

        // Wire confirm
        const newConfirm = confirmBtn.cloneNode(true);
        confirmBtn.parentNode.replaceChild(newConfirm, confirmBtn);
        newConfirm.addEventListener('click', () => this._submitReassign());

        // Wire cancel + backdrop
        const newCancel = cancelBtn.cloneNode(true);
        cancelBtn.parentNode.replaceChild(newCancel, cancelBtn);
        newCancel.addEventListener('click', () => this._closeReassignDialog());

        const newBackdrop = backdrop.cloneNode(true);
        backdrop.parentNode.replaceChild(newBackdrop, backdrop);
        newBackdrop.addEventListener('click', () => this._closeReassignDialog());

        dialog.classList.remove('hidden');
    },

    async _submitReassign() {
        const modeEl = document.getElementById('reassign-mode');
        const staffSelect = document.getElementById('reassign-target-staff');
        const reasonEl = document.getElementById('reassign-reason');
        const noteEl = document.getElementById('reassign-note');
        const confirmBtn = document.getElementById('reassign-confirm-btn');
        const errorEl = document.getElementById('reassign-error');

        const mode = modeEl ? modeEl.value : 'next_in_rotation';
        const targetStaff = (mode === 'target_staff' && staffSelect) ? staffSelect.value : '';
        const reason = reasonEl ? reasonEl.value : '';
        const note = noteEl ? noteEl.value : '';

        if (mode === 'target_staff' && !targetStaff) {
            if (errorEl) { errorEl.textContent = 'Please select a target staff member.'; errorEl.classList.remove('hidden'); }
            return;
        }

        if (confirmBtn) { confirmBtn.disabled = true; confirmBtn.textContent = 'Submitting...'; }
        if (errorEl) errorEl.classList.add('hidden');

        try {
            if (!this._reassignSamiRef) {
                throw new Error('Missing SAMI reference.');
            }
            await DashboardAPI.reassign(this._reassignSamiRef, mode, targetStaff, reason, note);
            this._closeReassignDialog();
            await this._openActiveModal();
        } catch (err) {
            if (errorEl) { errorEl.textContent = err.message; errorEl.classList.remove('hidden'); }
        } finally {
            if (confirmBtn) { confirmBtn.disabled = false; confirmBtn.textContent = 'Confirm Reassign'; }
        }
    },

    _closeReassignDialog() {
        const dialog = document.getElementById('reassign-dialog');
        if (dialog) dialog.classList.add('hidden');
        const errorEl = document.getElementById('reassign-error');
        if (errorEl) { errorEl.textContent = ''; errorEl.classList.add('hidden'); }
    },
};


/* Staff management panel — add / remove staff */

const StaffPanel = {
    _emailRegex: /^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$/,

    _domainBuckets: ['external_image_request', 'system_notification', 'always_hold', 'quarantine'],

    init() {
        const btn = document.getElementById('staff-add-btn');
        const input = document.getElementById('staff-email-input');
        if (btn) btn.addEventListener('click', () => this._addStaff());
        if (input) {
            input.addEventListener('keydown', (e) => {
                if (e.key === 'Enter') this._addStaff();
            });
        }

        // Initialize manager buttons
        const managerBtn = document.getElementById('manager-add-btn');
        const managerInput = document.getElementById('manager-email-input');
        if (managerBtn) managerBtn.addEventListener('click', () => this._addManager());
        if (managerInput) {
            managerInput.addEventListener('keydown', (e) => {
                if (e.key === 'Enter') this._addManager();
            });
        }

        // Initialize apps team buttons
        const appsBtn = document.getElementById('apps-add-btn');
        const appsInput = document.getElementById('apps-email-input');
        if (appsBtn) appsBtn.addEventListener('click', () => this._addApps());
        if (appsInput) {
            appsInput.addEventListener('keydown', (e) => {
                if (e.key === 'Enter') this._addApps();
            });
        }

        // Initialize domain bucket buttons
        for (const bucket of this._domainBuckets) {
            const domBtn = document.getElementById(`domain-${bucket}-add-btn`);
            const domInput = document.getElementById(`domain-${bucket}-input`);
            if (domBtn) domBtn.addEventListener('click', () => this._addDomain(bucket));
            if (domInput) {
                domInput.addEventListener('keydown', (e) => {
                    if (e.key === 'Enter') this._addDomain(bucket);
                });
            }
        }

        this.loadStaff();
        this.loadManagers();
        this.loadApps();
        for (const bucket of this._domainBuckets) {
            this.loadDomains(bucket);
        }
    },

    async loadStaff() {
        try {
            const data = await DashboardAPI.getStaff();
            this._renderList(data.staff);
        } catch (err) {
            console.error('Failed to load staff:', err);
        }
    },

    _renderList(staff) {
        const ul = document.getElementById('staff-list');
        if (!ul) return;

        ul.innerHTML = staff.map(email => {
            return `<li>
                <span class="staff-email">${this._esc(email)}</span>
                <button class="btn btn-remove" data-email="${this._esc(email)}">Remove</button>
            </li>`;
        }).join('');

        ul.querySelectorAll('.btn-remove').forEach(btn => {
            btn.addEventListener('click', () => this._removeStaff(btn.dataset.email));
        });
    },

    async _addStaff() {
        const input = document.getElementById('staff-email-input');
        const errorEl = document.getElementById('staff-add-error');
        if (!input) return;

        const email = input.value.trim().toLowerCase();
        if (!email) return;

        if (!this._emailRegex.test(email)) {
            this._showError('Invalid email format');
            return;
        }

        try {
            const data = await DashboardAPI.addStaff(email);
            this._renderList(data.staff);
            input.value = '';
            this._hideError();
        } catch (err) {
            this._showError(err.message);
        }
    },

    async _removeStaff(email) {
        try {
            const data = await DashboardAPI.removeStaff(email);
            this._renderList(data.staff);
            this._hideError();
        } catch (err) {
            this._showError(err.message);
        }
    },

    _showError(msg) {
        const el = document.getElementById('staff-add-error');
        if (el) {
            el.textContent = msg;
            el.classList.remove('hidden');
        }
    },

    _hideError() {
        const el = document.getElementById('staff-add-error');
        if (el) el.classList.add('hidden');
    },

    _esc(str) {
        const d = document.createElement('div');
        d.textContent = str || '';
        return d.innerHTML;
    },

    async loadManagers() {
        try {
            const data = await DashboardAPI.getManagers();
            this._renderManagerList(data.managers || []);
        } catch (err) {
            console.error('Failed to load managers:', err);
        }
    },

    _renderManagerList(managers) {
        const ul = document.getElementById('manager-list');
        if (!ul) return;

        ul.innerHTML = managers.map(email => {
            return `<li>
                <span class="staff-email">${this._esc(email)}</span>
                <button class="btn btn-remove" data-email="${this._esc(email)}">Remove</button>
            </li>`;
        }).join('');

        ul.querySelectorAll('.btn-remove').forEach(btn => {
            btn.addEventListener('click', () => this._removeManager(btn.dataset.email));
        });
    },

    async _addManager() {
        const input = document.getElementById('manager-email-input');
        const errorEl = document.getElementById('manager-add-error');
        if (!input) return;

        const email = input.value.trim().toLowerCase();
        if (!email) return;

        if (!this._emailRegex.test(email)) {
            this._showManagerError('Invalid email format');
            return;
        }

        try {
            const data = await DashboardAPI.addManager(email);
            this._renderManagerList(data.managers || []);
            input.value = '';
            this._hideManagerError();
        } catch (err) {
            this._showManagerError(err.message);
        }
    },

    async _removeManager(email) {
        try {
            const data = await DashboardAPI.removeManager(email);
            this._renderManagerList(data.managers || []);
            this._hideManagerError();
        } catch (err) {
            this._showManagerError(err.message);
        }
    },

    _showManagerError(msg) {
        const el = document.getElementById('manager-add-error');
        if (el) {
            el.textContent = msg;
            el.classList.remove('hidden');
        }
    },

    _hideManagerError() {
        const el = document.getElementById('manager-add-error');
        if (el) el.classList.add('hidden');
    },

    async loadApps() {
        try {
            const data = await DashboardAPI.getApps();
            this._renderAppsList(data.apps || []);
        } catch (err) {
            console.error('Failed to load apps team:', err);
        }
    },

    _renderAppsList(apps) {
        const ul = document.getElementById('apps-list');
        if (!ul) return;

        ul.innerHTML = apps.map(email => {
            return `<li>
                <span class="staff-email">${this._esc(email)}</span>
                <button class="btn btn-remove" data-email="${this._esc(email)}">Remove</button>
            </li>`;
        }).join('');

        ul.querySelectorAll('.btn-remove').forEach(btn => {
            btn.addEventListener('click', () => this._removeApps(btn.dataset.email));
        });
    },

    async _addApps() {
        const input = document.getElementById('apps-email-input');
        const errorEl = document.getElementById('apps-add-error');
        if (!input) return;

        const email = input.value.trim().toLowerCase();
        if (!email) return;

        if (!this._emailRegex.test(email)) {
            this._showAppsError('Invalid email format');
            return;
        }

        try {
            const data = await DashboardAPI.addApps(email);
            this._renderAppsList(data.apps || []);
            input.value = '';
            this._hideAppsError();
        } catch (err) {
            this._showAppsError(err.message);
        }
    },

    async _removeApps(email) {
        try {
            const data = await DashboardAPI.removeApps(email);
            this._renderAppsList(data.apps || []);
            this._hideAppsError();
        } catch (err) {
            this._showAppsError(err.message);
        }
    },

    _showAppsError(msg) {
        const el = document.getElementById('apps-add-error');
        if (el) {
            el.textContent = msg;
            el.classList.remove('hidden');
        }
    },

    _hideAppsError() {
        const el = document.getElementById('apps-add-error');
        if (el) el.classList.add('hidden');
    },

    // ── Domain management (generic across buckets) ──

    async loadDomains(bucket) {
        const listId = `domain-${bucket}-list`;
        try {
            const data = await DashboardAPI.getDomains(bucket);
            this._renderDomainList(bucket, listId, data.domains || []);
        } catch (err) {
            console.error(`Failed to load domains for ${bucket}:`, err);
        }
    },

    _renderDomainList(bucket, listId, domains) {
        const ul = document.getElementById(listId);
        if (!ul) return;

        ul.innerHTML = domains.map(domain => {
            return `<li>
                <span class="staff-email">${this._esc(domain)}</span>
                <button class="btn btn-remove" data-domain="${this._esc(domain)}">Remove</button>
            </li>`;
        }).join('');

        ul.querySelectorAll('.btn-remove').forEach(btn => {
            btn.addEventListener('click', () => this._removeDomain(bucket, btn.dataset.domain));
        });
    },

    async _addDomain(bucket) {
        const inputId = `domain-${bucket}-input`;
        const errorId = `domain-${bucket}-error`;
        const listId = `domain-${bucket}-list`;
        const input = document.getElementById(inputId);
        if (!input) return;

        const domain = input.value.trim().toLowerCase();
        if (!domain) return;

        if (!domain.includes('.')) {
            this._showDomainError(errorId, 'Domain must contain at least one dot');
            return;
        }

        try {
            const data = await DashboardAPI.addDomain(bucket, domain);
            this._renderDomainList(bucket, listId, data.domains || []);
            input.value = '';
            this._hideDomainError(errorId);
        } catch (err) {
            this._showDomainError(errorId, err.message);
        }
    },

    async _removeDomain(bucket, domain) {
        const errorId = `domain-${bucket}-error`;
        const listId = `domain-${bucket}-list`;
        try {
            const data = await DashboardAPI.removeDomain(bucket, domain);
            this._renderDomainList(bucket, listId, data.domains || []);
            this._hideDomainError(errorId);
        } catch (err) {
            this._showDomainError(errorId, err.message);
        }
    },

    _showDomainError(errorId, msg) {
        const el = document.getElementById(errorId);
        if (el) {
            el.textContent = msg;
            el.classList.remove('hidden');
        }
    },

    _hideDomainError(errorId) {
        const el = document.getElementById(errorId);
        if (el) el.classList.add('hidden');
    },
};

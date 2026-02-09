/* Main app — init + 15-second refresh loop + date range controls */

const App = {
    REFRESH_SEC: 15,
    _countdown: 15,
    _timer: null,
    _running: false,
    THEME_KEY: 'dashboard_theme',

    async init() {
        this._initThemeControls();
        KPITable.init();
        StaffPanel.init();
        ActivityFeed.init();
        this._initDateControls();

        // Keyboard shortcut: R to force refresh
        document.addEventListener('keydown', (e) => {
            if (e.key === 'r' || e.key === 'R') {
                if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;
                this.refresh();
            }
        });

        // Initial load
        await this.refresh();

        // Hide loading overlay after first successful load
        const overlay = document.getElementById('loading-overlay');
        if (overlay) overlay.classList.add('hidden');

        // Start refresh loop
        this._startLoop();
    },

    _initThemeControls() {
        const saved = localStorage.getItem(this.THEME_KEY);
        const initialTheme = saved === 'light' || saved === 'dark'
            ? saved
            : 'dark';

        this._setTheme(initialTheme, false);

        const toggle = document.getElementById('theme-toggle');
        if (!toggle) return;

        toggle.addEventListener('click', () => {
            const current = document.documentElement.dataset.theme === 'light' ? 'light' : 'dark';
            const next = current === 'dark' ? 'light' : 'dark';
            this._setTheme(next, true);
            Charts.applyTheme();
        });
    },

    _setTheme(theme, persist = true) {
        const normalized = theme === 'light' ? 'light' : 'dark';
        document.documentElement.dataset.theme = normalized;

        if (persist) {
            localStorage.setItem(this.THEME_KEY, normalized);
        }

        const toggle = document.getElementById('theme-toggle');
        if (toggle) {
            const next = normalized === 'dark' ? 'light' : 'dark';
            const label = next === 'light' ? 'Light' : 'Dark';
            toggle.textContent = label;
            toggle.setAttribute('aria-label', `Switch to ${next} theme`);
            toggle.setAttribute('title', `Switch to ${next} theme`);
        }
    },

    _getLocalDateString(date) {
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        return `${year}-${month}-${day}`;
    },

    _initDateControls() {
        const today = this._getLocalDateString(new Date());
        const startInput = document.getElementById('date-start');
        const endInput = document.getElementById('date-end');

        if (startInput) startInput.value = today;
        if (endInput) endInput.value = today;

        // Preset buttons
        document.querySelectorAll('.date-preset').forEach(btn => {
            btn.addEventListener('click', () => {
                // Remove active from all presets
                document.querySelectorAll('.date-preset').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');

                const preset = btn.dataset.preset;
                const todayDate = new Date();
                const todayStr = this._getLocalDateString(todayDate);

                if (preset === 'today') {
                    startInput.value = todayStr;
                    endInput.value = todayStr;
                } else if (preset === '7d') {
                    const d = new Date(todayDate);
                    d.setDate(d.getDate() - 6);
                    startInput.value = this._getLocalDateString(d);
                    endInput.value = todayStr;
                } else if (preset === '30d') {
                    const d = new Date(todayDate);
                    d.setDate(d.getDate() - 29);
                    startInput.value = this._getLocalDateString(d);
                    endInput.value = todayStr;
                } else if (preset === 'all') {
                    startInput.value = '';
                    endInput.value = '';
                }
                this.refresh();
            });
        });

        // Apply button
        const applyBtn = document.getElementById('date-apply-btn');
        if (applyBtn) applyBtn.addEventListener('click', () => this.refresh());

        // Enter key in date inputs
        [startInput, endInput].forEach(el => {
            if (el) el.addEventListener('keydown', (e) => {
                if (e.key === 'Enter') this.refresh();
            });
            // Also trigger on change
            if (el) el.addEventListener('change', () => this.refresh());
        });
    },

    _getDateParams() {
        const startInput = document.getElementById('date-start');
        const endInput = document.getElementById('date-end');
        const params = {};
        if (startInput && startInput.value) params.dateStart = startInput.value;
        if (endInput && endInput.value) params.dateEnd = endInput.value;
        return Object.keys(params).length > 0 ? params : null;
    },

    async refresh() {
        if (this._running) return;
        this._running = true;

        try {
            const params = this._getDateParams();
            const data = await DashboardAPI.getDashboard(params);

            SummaryCards.update(data.summary);
            KPITable.update(data.staff_kpis);
            Charts.update(data);
            ActivityFeed.update(data.activity_feed);
            LiveInsights.update(data);

            // Update range label
            const rangeLabel = document.getElementById('range-label');
            if (rangeLabel && data.date_start && data.date_end) {
                if (data.date_start === data.date_end) {
                    rangeLabel.textContent = data.date_start;
                } else {
                    rangeLabel.textContent = `${data.date_start} to ${data.date_end}`;
                }
            }

            // Update last-updated timestamp
            const lastEl = document.getElementById('last-updated');
            if (lastEl && data.last_updated) {
                const dt = new Date(data.last_updated);
                lastEl.textContent = `Updated ${dt.toLocaleTimeString()}`;
            }

            // Show warning if present
            const warnBanner = document.getElementById('warning-banner');
            const warnMsg = document.getElementById('warning-message');
            if (data.warning && warnBanner && warnMsg) {
                warnMsg.textContent = data.warning;
                warnBanner.classList.remove('hidden');
            } else if (warnBanner) {
                warnBanner.classList.add('hidden');
            }

            this._setConnectionStatus(true);

        } catch (err) {
            console.error('Refresh failed:', err);
            this._setConnectionStatus(false);

            const errBanner = document.getElementById('error-banner');
            const errMsg = document.getElementById('error-message');
            if (errBanner && errMsg) {
                errMsg.textContent = `Connection error: ${err.message}`;
                errBanner.classList.remove('hidden');
            }
        } finally {
            this._running = false;
            this._countdown = this.REFRESH_SEC;
        }
    },

    _startLoop() {
        this._timer = setInterval(() => {
            this._countdown--;
            const countEl = document.getElementById('countdown');
            if (countEl) countEl.textContent = `${this._countdown}s`;

            if (this._countdown <= 0) {
                this.refresh();
            }
        }, 1000);
    },

    _setConnectionStatus(connected) {
        const badge = document.getElementById('live-badge');
        if (!badge) return;
        if (connected) {
            badge.style.color = '';
            badge.querySelector('.pulse-dot').style.background = '';
            badge.querySelector('.pulse-dot').style.boxShadow = '';
        } else {
            badge.style.color = '#ef5350';
            badge.querySelector('.pulse-dot').style.background = '#ef5350';
            badge.querySelector('.pulse-dot').style.boxShadow = '0 0 8px #ef5350';
        }
    },
};

// Boot
document.addEventListener('DOMContentLoaded', () => App.init());

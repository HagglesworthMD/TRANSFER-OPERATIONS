/* Top-row summary metric cards */

const SummaryCards = {
    _prev: {},

    update(summary) {
        if (!summary) return;
        this._set('card-processed', summary.processed_today);
        this._set('card-completions', summary.completions_today);
        this._updateCompletionsDetail(summary);
        this._set('card-active', summary.active_count);
        this._set('card-active-staff', summary.active_staff);
        this._set('card-avg-time', summary.avg_time_human || 'N/A');
        this._set('card-uptime',
            summary.uptime || '—'
        );
        this._set('card-next-staff',
            summary.next_staff || '—'
        );
        this._updateHibCard(summary.hib_burst);
    },

    _updateCompletionsDetail(summary) {
        const detailEl = document.getElementById('card-completions-detail');
        if (!detailEl) return;

        const unmatched = Number(summary.completions_unmatched || 0);
        if (unmatched > 0) {
            detailEl.textContent = `+${unmatched} unmatched`;
            detailEl.classList.remove('ok');
            detailEl.classList.add('warn');
        } else {
            detailEl.textContent = 'All matched';
            detailEl.classList.remove('warn');
            detailEl.classList.add('ok');
        }
    },

    _updateHibCard(hibBurst) {
        if (!hibBurst) return;

        const valueEl = document.getElementById('card-hib');
        const detailEl = document.getElementById('card-hib-detail');
        if (!valueEl || !detailEl) return;

        // Update value (count)
        const count = hibBurst.count || 0;
        const strVal = String(count);
        if (valueEl.textContent !== strVal) {
            valueEl.textContent = strVal;
            valueEl.classList.remove('updated');
            void valueEl.offsetWidth;
            valueEl.classList.add('updated');
        }

        // Update detail text and color
        const status = hibBurst.status || 'normal';
        let detailText = '';

        if (status === 'burst') {
            detailText = '⚠ BURST! (15+ in 30min)';
        } else if (status === 'elevated') {
            detailText = 'Elevated (10-14 in 30min)';
        } else {
            detailText = 'Normal (0-9 in 30min)';
        }

        // Append last alert time if available
        if (hibBurst.last_alert_human) {
            detailText += ` • Last: ${hibBurst.last_alert_human}`;
        }

        detailEl.textContent = detailText;

        // Apply color class
        valueEl.classList.remove('hib-normal', 'hib-elevated', 'hib-burst');
        valueEl.classList.add(`hib-${status}`);
    },

    _set(id, value) {
        const el = document.getElementById(id);
        if (!el) return;
        const strVal = String(value);
        if (el.textContent !== strVal) {
            el.textContent = strVal;
            el.classList.remove('updated');
            // Force reflow for animation restart
            void el.offsetWidth;
            el.classList.add('updated');
            this._prev[id] = strVal;
        }
    },
};

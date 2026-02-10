/* Fetch wrapper for dashboard API */

const DashboardAPI = {
    base: '',

    async getDashboard(params) {
        let url = `${this.base}/api/dashboard`;
        if (params) {
            const qs = new URLSearchParams();
            if (params.dateStart) qs.set('date_start', params.dateStart);
            if (params.dateEnd) qs.set('date_end', params.dateEnd);
            const s = qs.toString();
            if (s) url += '?' + s;
        }
        const res = await fetch(url);
        if (!res.ok) throw new Error(`Dashboard API: ${res.status}`);
        return res.json();
    },

    async getStaff() {
        const res = await fetch(`${this.base}/api/staff`);
        if (!res.ok) throw new Error(`Staff API: ${res.status}`);
        return res.json();
    },

    async addStaff(email) {
        const res = await fetch(`${this.base}/api/staff`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ email }),
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.detail || 'Failed to add staff');
        return data;
    },

    async removeStaff(email) {
        const res = await fetch(`${this.base}/api/staff/${encodeURIComponent(email)}`, {
            method: 'DELETE',
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.detail || 'Failed to remove staff');
        return data;
    },

    async getHealth() {
        const res = await fetch(`${this.base}/api/health`);
        if (!res.ok) throw new Error(`Health API: ${res.status}`);
        return res.json();
    },

    async getManagers() {
        const res = await fetch(`${this.base}/api/managers`);
        if (!res.ok) throw new Error(`Managers API: ${res.status}`);
        return res.json();
    },

    async addManager(email) {
        const res = await fetch(`${this.base}/api/managers`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ email }),
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.detail || 'Failed to add manager');
        return data;
    },

    async removeManager(email) {
        const res = await fetch(`${this.base}/api/managers/${encodeURIComponent(email)}`, {
            method: 'DELETE',
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.detail || 'Failed to remove manager');
        return data;
    },

    async getApps() {
        const res = await fetch(`${this.base}/api/apps`);
        if (!res.ok) throw new Error(`Apps API: ${res.status}`);
        return res.json();
    },

    async addApps(email) {
        const res = await fetch(`${this.base}/api/apps`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ email }),
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.detail || 'Failed to add apps team member');
        return data;
    },

    async removeApps(email) {
        const res = await fetch(`${this.base}/api/apps/${encodeURIComponent(email)}`, {
            method: 'DELETE',
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.detail || 'Failed to remove apps team member');
        return data;
    },

    async getDomains(bucket) {
        const res = await fetch(`${this.base}/api/domains/${encodeURIComponent(bucket)}`);
        if (!res.ok) throw new Error(`Domains API: ${res.status}`);
        return res.json();
    },

    async addDomain(bucket, domain) {
        const res = await fetch(`${this.base}/api/domains/${encodeURIComponent(bucket)}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ domain }),
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.detail || 'Failed to add domain');
        return data;
    },

    async removeDomain(bucket, domain) {
        const res = await fetch(`${this.base}/api/domains/${encodeURIComponent(bucket)}/${encodeURIComponent(domain)}`, {
            method: 'DELETE',
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.detail || 'Failed to remove domain');
        return data;
    },
};

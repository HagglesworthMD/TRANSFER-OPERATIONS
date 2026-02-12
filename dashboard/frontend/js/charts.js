/* All Chart.js charts - pie, bar, doughnut */

const Charts = {
    _instances: {},
    _doughnutTitles: {},

    _colors: [
        '#42a5f5', '#66bb6a', '#ffca28', '#ef5350', '#ab47bc',
        '#26c6da', '#ff7043', '#8d6e63', '#78909c', '#5c6bc0',
    ],

    _riskColors: {
        critical: '#ef5350',
        urgent: '#ffca28',
        normal: '#42a5f5',
        low: '#66bb6a',
    },

    _sourceColors: {
        'Jones':    { bg: 'rgba(66, 165, 245, 0.7)',  border: '#42a5f5' },
        'Bensons':  { bg: 'rgba(102, 187, 106, 0.7)', border: '#66bb6a' },
        'System':   { bg: 'rgba(120, 144, 156, 0.7)', border: '#78909c' },
        'RadSA':    { bg: 'rgba(255, 202, 40, 0.7)',  border: '#ffca28' },
        'I-MED':    { bg: 'rgba(171, 71, 188, 0.7)',  border: '#ab47bc' },
        'Internal': { bg: 'rgba(38, 198, 218, 0.7)',  border: '#26c6da' },
    },

    _hourlyDetail: null,

    update(data) {
        if (!data) return;
        if (data.hourly_detail) {
            this._hourlyDetail = data.hourly_detail;
            this._updateHourlyStacked(data.hourly_detail);
        } else {
            this._updateHourlyLegacy(data.hourly);
        }
        this._updateDoughnut('chart-assignment-pie', data.assignment_pie, 'Assignments');
        this._updateDoughnut('chart-risk', data.risk_distribution, 'Risk Levels', this._riskColors);
        this._updateDoughnut('chart-domain', data.domain_distribution, 'Domains');
        this._updateDoughnut('chart-requestor', data.requestor_distribution, 'Requestors');
    },

    applyTheme() {
        Object.entries(this._instances).forEach(([id, chart]) => {
            if (!chart) return;

            if (chart.config.type === 'bar') {
                if (this._hourlyDetail) {
                    // Stacked chart — rebuild datasets with new theme colors
                    this._updateHourlyStacked(this._hourlyDetail);
                } else {
                    const barColors = this._barDatasetColors();
                    if (chart.data.datasets[0]) {
                        chart.data.datasets[0].backgroundColor = barColors.assignedBackground;
                        chart.data.datasets[0].borderColor = barColors.assignedBorder;
                    }
                    if (chart.data.datasets[1]) {
                        chart.data.datasets[1].backgroundColor = barColors.completedBackground;
                        chart.data.datasets[1].borderColor = barColors.completedBorder;
                    }
                    chart.options = this._barOptions();
                    chart.update('none');
                }
            } else if (chart.config.type === 'doughnut') {
                const title = this._doughnutTitles[id] || '';
                chart.data.datasets[0].borderColor = this._getCssVar('--chart-border', 'rgba(15, 25, 35, 0.8)');
                chart.options = this._doughnutOptions(title);
                chart.update('none');
            }
        });
    },

    _getSourceColor(source) {
        if (this._sourceColors[source]) return this._sourceColors[source];
        // Fallback: pick from color palette by hash
        let hash = 0;
        for (let i = 0; i < source.length; i++) hash = (hash * 31 + source.charCodeAt(i)) & 0x7fffffff;
        const base = this._colors[hash % this._colors.length];
        return { bg: base + 'b3', border: base };
    },

    _updateHourlyStacked(detail) {
        if (!detail || !detail.hours) return;
        const id = 'chart-hourly';
        const labels = Object.keys(detail.hours).sort();
        const sources = detail.all_sources || [];

        const datasets = sources.map(source => {
            const sc = this._getSourceColor(source);
            return {
                label: source,
                data: labels.map(h => (detail.hours[h]?.sources || {})[source] || 0),
                backgroundColor: sc.bg,
                borderColor: sc.border,
                borderWidth: 1,
                borderRadius: 4,
            };
        });

        const opts = this._stackedBarOptions();
        const self = this;

        if (this._instances[id]) {
            const chart = this._instances[id];
            chart.data.labels = labels;
            chart.data.datasets = datasets;
            chart.options = opts;
            chart.update('none');
            return;
        }

        const ctx = document.getElementById(id);
        if (!ctx) return;

        this._instances[id] = new Chart(ctx, {
            type: 'bar',
            data: { labels, datasets },
            options: opts,
        });

        // Click handler — dispatch custom event with clicked hour
        ctx.addEventListener('click', (evt) => {
            const chart = self._instances[id];
            if (!chart) return;
            const points = chart.getElementsAtEventForMode(evt, 'index', { intersect: false }, false);
            if (points.length > 0) {
                const hour = chart.data.labels[points[0].index];
                document.dispatchEvent(new CustomEvent('hourly-bar-click', { detail: { hour } }));
            }
        });
    },

    _updateHourlyLegacy(hourly) {
        if (!hourly) return;
        const id = 'chart-hourly';
        const labels = Object.keys(hourly);
        const assignedData = labels.map(k => hourly[k].assigned);
        const completedData = labels.map(k => hourly[k].completed);
        const barColors = this._barDatasetColors();

        if (this._instances[id]) {
            const chart = this._instances[id];
            chart.data.labels = labels;
            chart.data.datasets = [
                {
                    label: 'Assigned',
                    data: assignedData,
                    backgroundColor: barColors.assignedBackground,
                    borderColor: barColors.assignedBorder,
                    borderWidth: 1,
                    borderRadius: 4,
                },
                {
                    label: 'Completed',
                    data: completedData,
                    backgroundColor: barColors.completedBackground,
                    borderColor: barColors.completedBorder,
                    borderWidth: 1,
                    borderRadius: 4,
                },
            ];
            chart.options = this._barOptions();
            chart.update('none');
            return;
        }

        const ctx = document.getElementById(id);
        if (!ctx) return;

        this._instances[id] = new Chart(ctx, {
            type: 'bar',
            data: {
                labels,
                datasets: [
                    {
                        label: 'Assigned',
                        data: assignedData,
                        backgroundColor: barColors.assignedBackground,
                        borderColor: barColors.assignedBorder,
                        borderWidth: 1,
                        borderRadius: 4,
                    },
                    {
                        label: 'Completed',
                        data: completedData,
                        backgroundColor: barColors.completedBackground,
                        borderColor: barColors.completedBorder,
                        borderWidth: 1,
                        borderRadius: 4,
                    },
                ],
            },
            options: this._barOptions(),
        });
    },

    _updateDoughnut(id, dist, title, colorMap) {
        if (!dist) return;
        this._doughnutTitles[id] = title;

        const labels = Object.keys(dist);
        const values = Object.values(dist);
        const colors = colorMap
            ? labels.map(l => colorMap[l] || '#78909c')
            : labels.map((_, i) => this._colors[i % this._colors.length]);
        const borderColor = this._getCssVar('--chart-border', 'rgba(15, 25, 35, 0.8)');

        if (this._instances[id]) {
            const chart = this._instances[id];
            chart.data.labels = labels;
            chart.data.datasets[0].data = values;
            chart.data.datasets[0].backgroundColor = colors;
            chart.data.datasets[0].borderColor = borderColor;
            chart.options = this._doughnutOptions(title);
            chart.update('none');
            return;
        }

        const ctx = document.getElementById(id);
        if (!ctx) return;

        this._instances[id] = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels,
                datasets: [{
                    data: values,
                    backgroundColor: colors,
                    borderColor,
                    borderWidth: 2,
                }],
            },
            options: this._doughnutOptions(title),
        });
    },

    _barDatasetColors() {
        return {
            assignedBackground: this._getCssVar('--chart-assigned-bg', 'rgba(66, 165, 245, 0.7)'),
            assignedBorder: this._getCssVar('--chart-assigned-border', '#42a5f5'),
            completedBackground: this._getCssVar('--chart-completed-bg', 'rgba(102, 187, 106, 0.7)'),
            completedBorder: this._getCssVar('--chart-completed-border', '#66bb6a'),
        };
    },

    _barOptions() {
        return {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'top',
                    labels: { color: this._getCssVar('--chart-label', '#f4f8ff'), font: { size: 12 } },
                },
                tooltip: {
                    backgroundColor: this._getCssVar('--chart-tooltip-bg', 'rgba(10, 16, 24, 0.96)'),
                    titleColor: this._getCssVar('--chart-tooltip-title', '#f4f8ff'),
                    bodyColor: this._getCssVar('--chart-tooltip-body', '#f4f8ff'),
                    borderColor: this._getCssVar('--chart-tooltip-border', 'rgba(255, 255, 255, 0.14)'),
                    borderWidth: 1,
                    callbacks: {
                        label(ctx) {
                            const total = ctx.dataset.data.reduce((a, b) => a + b, 0);
                            const val = ctx.parsed.y;
                            const pct = total > 0 ? ((val / total) * 100).toFixed(1) : '0.0';
                            return `${ctx.dataset.label}: ${val} (${pct}% of total)`;
                        },
                    },
                },
            },
            scales: {
                x: {
                    ticks: { color: this._getCssVar('--chart-tick', '#eef3f8'), font: { size: 11 } },
                    grid: { color: this._getCssVar('--chart-grid-x', 'rgba(255,255,255,0.03)') },
                },
                y: {
                    ticks: { color: this._getCssVar('--chart-tick', '#eef3f8'), font: { size: 11 }, precision: 0 },
                    grid: { color: this._getCssVar('--chart-grid-y', 'rgba(255,255,255,0.05)') },
                    beginAtZero: true,
                },
            },
        };
    },

    _stackedBarOptions() {
        const base = this._barOptions();
        base.scales.x.stacked = true;
        base.scales.y.stacked = true;
        base.plugins.tooltip.mode = 'index';
        base.plugins.tooltip.intersect = false;
        base.onClick = (evt, elements, chart) => {
            if (elements.length > 0) {
                const hour = chart.data.labels[elements[0].index];
                document.dispatchEvent(new CustomEvent('hourly-bar-click', { detail: { hour } }));
            }
        };
        return base;
    },

    _doughnutOptions(title) {
        return {
            responsive: true,
            maintainAspectRatio: false,
            cutout: '55%',
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: {
                        color: this._getCssVar('--chart-label', '#f4f8ff'),
                        font: { size: 12 },
                        padding: 12,
                        boxWidth: 12,
                        usePointStyle: true,
                        pointStyle: 'circle',
                        generateLabels(chart) {
                            const data = chart.data;
                            if (!data.labels.length) return [];
                            const total = data.datasets[0].data.reduce((a, b) => a + b, 0);
                            return data.labels.map((label, i) => {
                                const val = data.datasets[0].data[i];
                                const pct = total > 0 ? ((val / total) * 100).toFixed(1) : '0.0';
                                return {
                                    text: `${label} (${pct}%)`,
                                    fillStyle: data.datasets[0].backgroundColor[i],
                                    strokeStyle: data.datasets[0].borderColor || 'transparent',
                                    lineWidth: 0,
                                    hidden: !chart.getDataVisibility(i),
                                    index: i,
                                };
                            });
                        },
                    },
                },
                tooltip: {
                    backgroundColor: this._getCssVar('--chart-tooltip-bg', 'rgba(10, 16, 24, 0.96)'),
                    titleColor: this._getCssVar('--chart-tooltip-title', '#f4f8ff'),
                    bodyColor: this._getCssVar('--chart-tooltip-body', '#f4f8ff'),
                    borderColor: this._getCssVar('--chart-tooltip-border', 'rgba(255, 255, 255, 0.14)'),
                    borderWidth: 1,
                    callbacks: {
                        label(ctx) {
                            const total = ctx.dataset.data.reduce((a, b) => a + b, 0);
                            const val = ctx.parsed;
                            const pct = total > 0 ? ((val / total) * 100).toFixed(1) : '0.0';
                            return `${ctx.label}: ${val} (${pct}%)`;
                        },
                    },
                },
                title: {
                    display: false,
                    text: title,
                },
            },
        };
    },

    _getCssVar(name, fallback) {
        const value = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
        return value || fallback;
    },
};

/**
 * SUNTER Dashboard - Professional Chart Helpers
 * Optimized for Charts.js 4.x with Mobile-First Design
 */

const ChartHelpers = {
    /**
     * Modern Palette - Sync with CSS Variables
     */
    colors: {
        primary: '#2563eb', // Blue
        success: '#10b981', // Green
        warning: '#f59e0b', // Amber
        danger: '#ef4444',  // Red
        info: '#06b6d4',    // Cyan
        purple: '#8b5cf6',
        gray: '#94a3b8'
    },
    
    palette: [
        '#2563eb', '#10b981', '#f59e0b', '#ef4444', 
        '#06b6d4', '#8b5cf6', '#ec4899', '#94a3b8'
    ],
    
    /**
     * Global Default Options for Clean UI
     */
    defaultOptions: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: {
                display: true,
                position: 'bottom',
                labels: {
                    padding: 20,
                    usePointStyle: true,
                    pointStyle: 'circle',
                    font: { size: 11, weight: '600', family: "'Inter', sans-serif" },
                    color: '#64748b'
                }
            },
            tooltip: {
                backgroundColor: '#1e293b',
                padding: 12,
                cornerRadius: 8,
                titleFont: { size: 13, weight: 'bold' },
                bodyFont: { size: 12 },
                displayColors: true,
                boxPadding: 6
            }
        },
        scales: {
            x: {
                grid: { display: false },
                ticks: { color: '#94a3b8', font: { size: 10 } }
            },
            y: {
                border: { display: false },
                grid: { color: '#f1f5f9' },
                ticks: { color: '#94a3b8', font: { size: 10 } }
            }
        }
    },
    
    /**
     * Create Modern Line Chart (with area gradient)
     */
    createLineChart: function(ctx, data, options = {}) {
        return new Chart(ctx, {
            type: 'line',
            data: {
                labels: data.labels,
                datasets: data.datasets.map((dataset, i) => {
                    const color = dataset.color || this.palette[i % this.palette.length];
                    return {
                        label: dataset.label,
                        data: dataset.data,
                        borderColor: color,
                        backgroundColor: this.hexToRgba(color, 0.1),
                        borderWidth: 3,
                        tension: 0.4,
                        fill: true,
                        pointRadius: 0,
                        pointHoverRadius: 6,
                        pointHoverBackgroundColor: color,
                        pointHoverBorderColor: '#fff',
                        pointHoverBorderWidth: 2,
                        ...dataset
                    };
                })
            },
            options: this.mergeOptions(this.defaultOptions, options)
        });
    },
    
    /**
     * Create Rounded Bar Chart
     */
    createBarChart: function(ctx, data, options = {}) {
        return new Chart(ctx, {
            type: 'bar',
            data: {
                labels: data.labels,
                datasets: data.datasets.map((dataset, i) => ({
                    label: dataset.label,
                    data: dataset.data,
                    backgroundColor: dataset.color || this.palette[i % this.palette.length],
                    borderRadius: 6,
                    maxBarThickness: 30,
                    ...dataset
                }))
            },
            options: this.mergeOptions(this.defaultOptions, options)
        });
    },
    
    /**
     * Create Professional Doughnut Chart
     */
    createPieChart: function(ctx, data, options = {}, type = 'doughnut') {
        return new Chart(ctx, {
            type: type,
            data: {
                labels: data.labels,
                datasets: [{
                    data: data.data,
                    backgroundColor: data.colors || this.palette,
                    borderWidth: 0,
                    hoverOffset: 10
                }]
            },
            options: this.mergeOptions(this.defaultOptions, {
                cutout: type === 'doughnut' ? '75%' : 0,
                ...options
            })
        });
    },

    /**
     * Helpers: Color & Formatters
     */
    mergeOptions: function(defaults, custom) {
        return {
            ...defaults,
            ...custom,
            plugins: { ...defaults.plugins, ...custom.plugins },
            scales: { ...defaults.scales, ...custom.scales }
        };
    },

    hexToRgba: function(hex, alpha = 1) {
        const r = parseInt(hex.slice(1, 3), 16);
        const g = parseInt(hex.slice(3, 5), 16);
        const b = parseInt(hex.slice(5, 7), 16);
        return `rgba(${r}, ${g}, ${b}, ${alpha})`;
    },

    /**
     * Smart Value Formatter (ex: 1.5M, 200K)
     */
    formatValue: function(value) {
        if (value >= 1000000000) return (value / 1000000000).toFixed(1) + 'B';
        if (value >= 1000000) return (value / 1000000).toFixed(1) + 'M';
        if (value >= 1000) return (value / 1000).toFixed(0) + 'K';
        return value;
    }
};

// Bind globals
window.ChartHelpers = ChartHelpers;
window.createLineChart = ChartHelpers.createLineChart.bind(ChartHelpers);
window.createBarChart = ChartHelpers.createBarChart.bind(ChartHelpers);
window.createPieChart = ChartHelpers.createPieChart.bind(ChartHelpers);

console.log('✅ Professional Charts.js Ready');

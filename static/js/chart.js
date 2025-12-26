/**
 * SUNTER Dashboard - Chart Helpers
 * Helper functions for Chart.js
 */

const ChartHelpers = {
    /**
     * Default chart colors
     */
    colors: {
        primary: '#1e40af',
        success: '#10b981',
        warning: '#f59e0b',
        danger: '#ef4444',
        info: '#06b6d4',
        purple: '#8b5cf6',
        pink: '#ec4899',
        gray: '#6b7280'
    },
    
    /**
     * Color palette for multiple series
     */
    palette: [
        '#1e40af', '#10b981', '#f59e0b', '#ef4444', 
        '#06b6d4', '#8b5cf6', '#ec4899', '#6b7280'
    ],
    
    /**
     * Default chart options
     */
    defaultOptions: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: {
                display: true,
                position: 'bottom',
                labels: {
                    padding: 15,
                    usePointStyle: true,
                    font: {
                        size: 12
                    }
                }
            },
            tooltip: {
                backgroundColor: 'rgba(0, 0, 0, 0.8)',
                padding: 12,
                cornerRadius: 8,
                titleFont: {
                    size: 13,
                    weight: 'bold'
                },
                bodyFont: {
                    size: 12
                }
            }
        }
    },
    
    /**
     * Create line chart
     */
    createLineChart: function(ctx, data, options = {}) {
        return new Chart(ctx, {
            type: 'line',
            data: {
                labels: data.labels,
                datasets: data.datasets.map((dataset, i) => ({
                    label: dataset.label,
                    data: dataset.data,
                    borderColor: dataset.color || this.palette[i],
                    backgroundColor: this.hexToRgba(dataset.color || this.palette[i], 0.1),
                    borderWidth: 2,
                    tension: 0.4,
                    fill: dataset.fill !== false,
                    pointRadius: 3,
                    pointHoverRadius: 5,
                    ...dataset
                }))
            },
            options: this.mergeOptions(this.defaultOptions, {
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: {
                            callback: function(value) {
                                return options.yFormat ? options.yFormat(value) : value;
                            }
                        }
                    }
                },
                ...options
            })
        });
    },
    
    /**
     * Create bar chart
     */
    createBarChart: function(ctx, data, options = {}) {
        return new Chart(ctx, {
            type: 'bar',
            data: {
                labels: data.labels,
                datasets: data.datasets.map((dataset, i) => ({
                    label: dataset.label,
                    data: dataset.data,
                    backgroundColor: dataset.color || this.palette[i],
                    borderColor: dataset.color || this.palette[i],
                    borderWidth: 1,
                    borderRadius: 6,
                    ...dataset
                }))
            },
            options: this.mergeOptions(this.defaultOptions, {
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: {
                            callback: function(value) {
                                return options.yFormat ? options.yFormat(value) : value;
                            }
                        }
                    }
                },
                ...options
            })
        });
    },
    
    /**
     * Create pie/doughnut chart
     */
    createPieChart: function(ctx, data, options = {}, type = 'doughnut') {
        return new Chart(ctx, {
            type: type,
            data: {
                labels: data.labels,
                datasets: [{
                    data: data.data,
                    backgroundColor: data.colors || this.palette,
                    borderWidth: 2,
                    borderColor: '#fff'
                }]
            },
            options: this.mergeOptions(this.defaultOptions, {
                cutout: type === 'doughnut' ? '70%' : 0,
                plugins: {
                    legend: {
                        position: 'bottom'
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                const label = context.label || '';
                                const value = context.parsed || 0;
                                const total = context.dataset.data.reduce((a, b) => a + b, 0);
                                const percentage = ((value / total) * 100).toFixed(1);
                                return `${label}: ${options.valueFormat ? options.valueFormat(value) : value} (${percentage}%)`;
                            }
                        }
                    }
                },
                ...options
            })
        });
    },
    
    /**
     * Create horizontal bar chart
     */
    createHorizontalBarChart: function(ctx, data, options = {}) {
        return new Chart(ctx, {
            type: 'bar',
            data: {
                labels: data.labels,
                datasets: data.datasets.map((dataset, i) => ({
                    label: dataset.label,
                    data: dataset.data,
                    backgroundColor: dataset.color || this.palette[i],
                    borderColor: dataset.color || this.palette[i],
                    borderWidth: 1,
                    borderRadius: 6,
                    ...dataset
                }))
            },
            options: this.mergeOptions(this.defaultOptions, {
                indexAxis: 'y',
                scales: {
                    x: {
                        beginAtZero: true,
                        ticks: {
                            callback: function(value) {
                                return options.xFormat ? options.xFormat(value) : value;
                            }
                        }
                    }
                },
                ...options
            })
        });
    },
    
    /**
     * Merge chart options
     */
    mergeOptions: function(defaults, custom) {
        return {
            ...defaults,
            ...custom,
            plugins: {
                ...defaults.plugins,
                ...custom.plugins
            },
            scales: {
                ...defaults.scales,
                ...custom.scales
            }
        };
    },
    
    /**
     * Convert hex to rgba
     */
    hexToRgba: function(hex, alpha = 1) {
        const r = parseInt(hex.slice(1, 3), 16);
        const g = parseInt(hex.slice(3, 5), 16);
        const b = parseInt(hex.slice(5, 7), 16);
        return `rgba(${r}, ${g}, ${b}, ${alpha})`;
    },
    
    /**
     * Destroy chart if exists
     */
    destroyChart: function(chart) {
        if (chart && typeof chart.destroy === 'function') {
            chart.destroy();
        }
    },
    
    /**
     * Format currency for chart tooltip
     */
    formatCurrency: function(value) {
        if (value >= 1000000) {
            return 'Rp ' + (value / 1000000).toFixed(1) + 'M';
        } else if (value >= 1000) {
            return 'Rp ' + (value / 1000).toFixed(0) + 'K';
        }
        return 'Rp ' + value.toFixed(0);
    },
    
    /**
     * Format number for chart
     */
    formatNumber: function(value) {
        if (value >= 1000000) {
            return (value / 1000000).toFixed(1) + 'M';
        } else if (value >= 1000) {
            return (value / 1000).toFixed(0) + 'K';
        }
        return value.toString();
    },
    
    /**
     * Generate gradient background
     */
    createGradient: function(ctx, color1, color2) {
        const gradient = ctx.createLinearGradient(0, 0, 0, 400);
        gradient.addColorStop(0, color1);
        gradient.addColorStop(1, color2);
        return gradient;
    }
};

// Make ChartHelpers global
window.ChartHelpers = ChartHelpers;

// Quick chart creation functions
window.createLineChart = ChartHelpers.createLineChart.bind(ChartHelpers);
window.createBarChart = ChartHelpers.createBarChart.bind(ChartHelpers);
window.createPieChart = ChartHelpers.createPieChart.bind(ChartHelpers);
window.createHorizontalBarChart = ChartHelpers.createHorizontalBarChart.bind(ChartHelpers);

console.log('✅ Charts.js loaded successfully');

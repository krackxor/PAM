/**
 * Chart Utilities
 * Common chart configurations and helpers
 */

// Chart.js default configuration
Chart.defaults.font.family = '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif';
Chart.defaults.font.size = 13;
Chart.defaults.color = '#6c757d';

/**
 * Create line chart
 */
function createLineChart(canvasId, labels, data, options = {}) {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return null;
    
    const defaultOptions = {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: {
                display: options.showLegend !== false,
                position: 'bottom'
            },
            tooltip: {
                mode: 'index',
                intersect: false,
                callbacks: {
                    label: function(context) {
                        let label = context.dataset.label || '';
                        if (label) {
                            label += ': ';
                        }
                        if (options.formatValue) {
                            label += options.formatValue(context.parsed.y);
                        } else {
                            label += context.parsed.y.toLocaleString('id-ID');
                        }
                        return label;
                    }
                }
            }
        },
        scales: {
            y: {
                beginAtZero: true,
                ticks: {
                    callback: function(value) {
                        if (options.formatYAxis) {
                            return options.formatYAxis(value);
                        }
                        return value.toLocaleString('id-ID');
                    }
                }
            }
        }
    };
    
    return new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: options.label || 'Data',
                data: data,
                borderColor: options.borderColor || '#2563eb',
                backgroundColor: options.backgroundColor || 'rgba(37, 99, 235, 0.1)',
                tension: 0.4,
                fill: true
            }]
        },
        options: { ...defaultOptions, ...options.chartOptions }
    });
}

/**
 * Create bar chart
 */
function createBarChart(canvasId, labels, data, options = {}) {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return null;
    
    return new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: options.label || 'Data',
                data: data,
                backgroundColor: options.backgroundColor || '#2563eb'
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: options.showLegend !== false
                }
            },
            scales: {
                y: {
                    beginAtZero: true
                }
            }
        }
    });
}

/**
 * Create pie/doughnut chart
 */
function createPieChart(canvasId, labels, data, options = {}) {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return null;
    
    const type = options.type || 'pie';
    
    return new Chart(ctx, {
        type: type,
        data: {
            labels: labels,
            datasets: [{
                data: data,
                backgroundColor: options.colors || [
                    '#2563eb', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899'
                ]
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: options.legendPosition || 'bottom'
                }
            }
        }
    });
}

/**
 * Format Rupiah for charts
 */
function formatRupiahChart(value) {
    return 'Rp ' + (value / 1000000).toFixed(1) + 'M';
}

/**
 * Destroy chart if exists
 */
function destroyChart(chartInstance) {
    if (chartInstance) {
        chartInstance.destroy();
    }
    return null;
}

/**
 * Update chart data
 */
function updateChartData(chartInstance, labels, data) {
    if (!chartInstance) return;
    
    chartInstance.data.labels = labels;
    chartInstance.data.datasets[0].data = data;
    chartInstance.update();
}

// Export for use in other files
window.ChartUtils = {
    createLineChart,
    createBarChart,
    createPieChart,
    formatRupiahChart,
    destroyChart,
    updateChartData
};

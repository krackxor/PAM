/**
 * Collection Dashboard JavaScript
 * Handles collection-specific functionality
 */

(function() {
    'use strict';
    
    let collectionChart = null;
    let collectionTable = null;
    
    // Initialize
    $(document).ready(function() {
        initCollectionDashboard();
    });
    
    function initCollectionDashboard() {
        // Load initial data
        loadCollectionData();
        
        // Event listeners
        $('#loadCollectionBtn').on('click', loadCollectionData);
        $('#exportCollectionBtn').on('click', exportCollectionData);
    }
    
    async function loadCollectionData() {
        const bulan = $('#bulan').val();
        const tahun = $('#tahun').val();
        
        if (!bulan || !tahun) {
            alert('Please select period');
            return;
        }
        
        try {
            showLoading();
            
            // Fetch daily collection
            const response = await fetch(`/api/collection/daily?bulan=${bulan}&tahun=${tahun}`);
            const data = await response.json();
            
            if (!response.ok) {
                throw new Error(data.error || 'Failed to load data');
            }
            
            // Update chart
            updateCollectionChart(data);
            
            // Update table
            updateCollectionTable(data);
            
            // Update summary
            updateCollectionSummary(data);
            
            hideLoading();
            showToast('Data loaded successfully', 'success');
            
        } catch (error) {
            hideLoading();
            showToast('Error: ' + error.message, 'error');
            console.error(error);
        }
    }
    
    function updateCollectionChart(data) {
        const labels = data.map(d => d.date);
        const totals = data.map(d => d.total);
        
        if (collectionChart) {
            window.ChartUtils.destroyChart(collectionChart);
        }
        
        collectionChart = window.ChartUtils.createLineChart('collectionChart', labels, totals, {
            label: 'Collection',
            formatYAxis: window.ChartUtils.formatRupiahChart
        });
    }
    
    function updateCollectionTable(data) {
        if (collectionTable) {
            collectionTable.destroy();
        }
        
        const tbody = $('#collectionTable tbody');
        
        if (data.length === 0) {
            tbody.html('<tr><td colspan="5" class="text-center text-muted">No data available</td></tr>');
            return;
        }
        
        tbody.html(data.map(item => `
            <tr>
                <td>${item.date}</td>
                <td>${item.transactions || 0}</td>
                <td class="text-right">${formatRupiah(item.current || 0)}</td>
                <td class="text-right">${formatRupiah(item.tunggakan || 0)}</td>
                <td class="text-right"><strong>${formatRupiah(item.total || 0)}</strong></td>
            </tr>
        `).join(''));
        
        collectionTable = $('#collectionTable').DataTable({
            pageLength: 25,
            order: [[0, 'desc']],
            language: {
                search: "Search:",
                lengthMenu: "Show _MENU_",
                info: "_START_-_END_ of _TOTAL_"
            }
        });
    }
    
    function updateCollectionSummary(data) {
        const total = data.reduce((sum, item) => sum + (item.total || 0), 0);
        const current = data.reduce((sum, item) => sum + (item.current || 0), 0);
        const tunggakan = data.reduce((sum, item) => sum + (item.tunggakan || 0), 0);
        const transactions = data.reduce((sum, item) => sum + (item.transactions || 0), 0);
        
        $('#totalCollection').text(formatRupiah(total));
        $('#currentCollection').text(formatRupiah(current));
        $('#tunggakanCollection').text(formatRupiah(tunggakan));
        $('#totalTransactions').text(formatNumber(transactions));
    }
    
    async function exportCollectionData() {
        const bulan = $('#bulan').val();
        const tahun = $('#tahun').val();
        
        try {
            const response = await fetch(`/api/collection/export?bulan=${bulan}&tahun=${tahun}`);
            const blob = await response.blob();
            
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `collection_${bulan}_${tahun}.csv`;
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            a.remove();
            
            showToast('Data exported successfully', 'success');
            
        } catch (error) {
            showToast('Export failed: ' + error.message, 'error');
        }
    }
    
    // Helper functions
    function formatRupiah(amount) {
        return new Intl.NumberFormat('id-ID', {
            style: 'currency',
            currency: 'IDR',
            minimumFractionDigits: 0
        }).format(amount);
    }
    
    function formatNumber(num) {
        return new Intl.NumberFormat('id-ID').format(num);
    }
    
    function showLoading() {
        $('#loadingOverlay').show();
    }
    
    function hideLoading() {
        $('#loadingOverlay').hide();
    }
    
    function showToast(message, type = 'info') {
        if (window.SunterDashboard && window.SunterDashboard.toast) {
            window.SunterDashboard.toast[type](message);
        } else {
            alert(message);
        }
    }
    
})();

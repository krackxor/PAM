/**
 * SUNTER Dashboard - Main JavaScript
 * Global utilities and functions
 */

// Global App object
const App = {
    /**
     * Format number to Rupiah currency
     */
    formatRupiah: function(amount) {
        if (!amount && amount !== 0) return 'Rp 0';
        return new Intl.NumberFormat('id-ID', {
            style: 'currency',
            currency: 'IDR',
            minimumFractionDigits: 0,
            maximumFractionDigits: 0
        }).format(amount);
    },
    
    /**
     * Format number with thousand separator
     */
    formatNumber: function(num) {
        if (!num && num !== 0) return '0';
        return new Intl.NumberFormat('id-ID').format(num);
    },
    
    /**
     * Format percentage
     */
    formatPercent: function(num, decimals = 1) {
        if (!num && num !== 0) return '0%';
        return num.toFixed(decimals) + '%';
    },
    
    /**
     * Show loading overlay
     */
    showLoading: function() {
        let loader = document.getElementById('pageLoader');
        if (!loader) {
            loader = document.createElement('div');
            loader.id = 'pageLoader';
            loader.className = 'page-loader';
            loader.innerHTML = '<div class="spinner"></div>';
            document.body.appendChild(loader);
        }
        loader.style.display = 'flex';
    },
    
    /**
     * Hide loading overlay
     */
    hideLoading: function() {
        const loader = document.getElementById('pageLoader');
        if (loader) {
            loader.style.display = 'none';
        }
    },
    
    /**
     * Show toast notification
     */
    toast: function(message, type = 'info', duration = 3000) {
        // Remove existing toasts
        const existing = document.querySelectorAll('.toast-notification');
        existing.forEach(t => t.remove());
        
        // Create toast
        const toast = document.createElement('div');
        toast.className = `toast-notification toast-${type}`;
        
        // Icon based on type
        const icons = {
            success: 'fa-check-circle',
            error: 'fa-exclamation-circle',
            warning: 'fa-exclamation-triangle',
            info: 'fa-info-circle'
        };
        
        toast.innerHTML = `
            <i class="fas ${icons[type] || icons.info}"></i>
            <span>${message}</span>
        `;
        
        document.body.appendChild(toast);
        
        // Show toast
        setTimeout(() => toast.classList.add('show'), 10);
        
        // Auto hide
        setTimeout(() => {
            toast.classList.remove('show');
            setTimeout(() => toast.remove(), 300);
        }, duration);
    },
    
    /**
     * API call wrapper with loading and error handling
     */
    api: async function(url, options = {}) {
        try {
            this.showLoading();
            
            const response = await fetch(url, {
                ...options,
                headers: {
                    'Content-Type': 'application/json',
                    ...options.headers
                }
            });
            
            const data = await response.json();
            
            if (!response.ok) {
                throw new Error(data.error || 'Request failed');
            }
            
            return data;
            
        } catch (error) {
            console.error('API Error:', error);
            this.toast(error.message, 'error');
            throw error;
        } finally {
            this.hideLoading();
        }
    },
    
    /**
     * Debounce function for search/input
     */
    debounce: function(func, wait) {
        let timeout;
        return function executedFunction(...args) {
            const later = () => {
                clearTimeout(timeout);
                func(...args);
            };
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
        };
    },
    
    /**
     * Get current date
     */
    getCurrentDate: function() {
        return new Date();
    },
    
    /**
     * Get current month and year
     */
    getCurrentPeriode: function() {
        const now = new Date();
        return {
            bulan: now.getMonth() + 1,
            tahun: now.getFullYear()
        };
    },
    
    /**
     * Format date to Indonesia format
     */
    formatDate: function(date) {
        if (!date) return '-';
        const d = new Date(date);
        return d.toLocaleDateString('id-ID', {
            day: 'numeric',
            month: 'long',
            year: 'numeric'
        });
    },
    
    /**
     * Format date to short format
     */
    formatDateShort: function(date) {
        if (!date) return '-';
        const d = new Date(date);
        return d.toLocaleDateString('id-ID', {
            day: '2-digit',
            month: '2-digit',
            year: 'numeric'
        });
    },
    
    /**
     * Confirm dialog
     */
    confirm: function(message) {
        return window.confirm(message);
    },
    
    /**
     * Copy to clipboard
     */
    copyToClipboard: function(text) {
        if (navigator.clipboard) {
            navigator.clipboard.writeText(text).then(() => {
                this.toast('Copied to clipboard', 'success');
            });
        } else {
            // Fallback
            const textarea = document.createElement('textarea');
            textarea.value = text;
            document.body.appendChild(textarea);
            textarea.select();
            document.execCommand('copy');
            document.body.removeChild(textarea);
            this.toast('Copied to clipboard', 'success');
        }
    },
    
    /**
     * Download file
     */
    download: function(url, filename) {
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
    },
    
    /**
     * Export table to CSV
     */
    exportTableToCSV: function(tableId, filename = 'export.csv') {
        const table = document.getElementById(tableId);
        if (!table) {
            this.toast('Table not found', 'error');
            return;
        }
        
        const rows = Array.from(table.querySelectorAll('tr'));
        const csv = rows.map(row => {
            const cells = Array.from(row.querySelectorAll('th, td'));
            return cells.map(cell => {
                let text = cell.textContent.trim();
                // Escape quotes
                text = text.replace(/"/g, '""');
                // Wrap in quotes if contains comma
                if (text.includes(',') || text.includes('"')) {
                    text = `"${text}"`;
                }
                return text;
            }).join(',');
        }).join('\n');
        
        // Create blob and download
        const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
        const url = URL.createObjectURL(blob);
        this.download(url, filename);
        URL.revokeObjectURL(url);
        
        this.toast('Export successful', 'success');
    }
};

// Make App global
window.App = App;

// DOM Ready
document.addEventListener('DOMContentLoaded', function() {
    console.log('🚀 SUNTER Dashboard Loaded');
    
    // Initialize navigation active state
    initNavigation();
    
    // Initialize tooltips (if any)
    initTooltips();
    
    // Initialize pull to refresh (mobile)
    initPullToRefresh();
    
    // Initialize keyboard shortcuts
    initKeyboardShortcuts();
});

/**
 * Initialize navigation active state
 */
function initNavigation() {
    const currentPath = window.location.pathname;
    
    // Bottom nav
    document.querySelectorAll('.bottom-nav-item, .nav-item').forEach(item => {
        const href = item.getAttribute('href');
        if (href === currentPath || (href !== '/' && currentPath.startsWith(href))) {
            item.classList.add('active');
        }
    });
    
    // Desktop nav
    document.querySelectorAll('.navbar-nav .nav-link').forEach(link => {
        const href = link.getAttribute('href');
        if (href === currentPath || (href !== '/' && currentPath.startsWith(href))) {
            link.classList.add('active');
        }
    });
}

/**
 * Initialize tooltips
 */
function initTooltips() {
    // Add title attribute handling
    document.querySelectorAll('[data-tooltip]').forEach(el => {
        el.addEventListener('mouseenter', function() {
            const tooltip = document.createElement('div');
            tooltip.className = 'tooltip-popup';
            tooltip.textContent = this.getAttribute('data-tooltip');
            document.body.appendChild(tooltip);
            
            const rect = this.getBoundingClientRect();
            tooltip.style.top = (rect.top - tooltip.offsetHeight - 5) + 'px';
            tooltip.style.left = (rect.left + rect.width / 2 - tooltip.offsetWidth / 2) + 'px';
            
            this._tooltip = tooltip;
        });
        
        el.addEventListener('mouseleave', function() {
            if (this._tooltip) {
                this._tooltip.remove();
                this._tooltip = null;
            }
        });
    });
}

/**
 * Initialize pull to refresh (mobile)
 */
function initPullToRefresh() {
    let startY = 0;
    let isPulling = false;
    
    document.addEventListener('touchstart', function(e) {
        if (window.scrollY === 0) {
            startY = e.touches[0].clientY;
            isPulling = true;
        }
    });
    
    document.addEventListener('touchmove', function(e) {
        if (!isPulling) return;
        
        const currentY = e.touches[0].clientY;
        const diff = currentY - startY;
        
        if (diff > 100) {
            // Show refresh indicator
            console.log('Pull to refresh triggered');
        }
    });
    
    document.addEventListener('touchend', function(e) {
        if (isPulling) {
            const currentY = e.changedTouches[0].clientY;
            const diff = currentY - startY;
            
            if (diff > 100) {
                location.reload();
            }
            
            isPulling = false;
        }
    });
}

/**
 * Initialize keyboard shortcuts
 */
function initKeyboardShortcuts() {
    document.addEventListener('keydown', function(e) {
        // Ctrl/Cmd + K: Focus search (if exists)
        if ((e.ctrlKey || e.metaKey) && e.key === 'k') {
            e.preventDefault();
            const search = document.querySelector('input[type="search"], input[placeholder*="Cari"]');
            if (search) search.focus();
        }
        
        // Escape: Close modals/dialogs
        if (e.key === 'Escape') {
            const modals = document.querySelectorAll('.modal.show, .dialog.show');
            modals.forEach(m => m.classList.remove('show'));
        }
    });
}

/**
 * Tab switching helper
 */
function initTabs(tabSelector = '.tab', contentSelector = '.tab-content') {
    document.querySelectorAll(tabSelector).forEach(tab => {
        tab.addEventListener('click', function() {
            // Remove active from all tabs
            document.querySelectorAll(tabSelector).forEach(t => t.classList.remove('active'));
            
            // Add active to clicked tab
            this.classList.add('active');
            
            // Hide all content
            document.querySelectorAll(contentSelector).forEach(c => c.style.display = 'none');
            
            // Show selected content
            const target = this.getAttribute('data-tab');
            const content = document.getElementById('tab-' + target);
            if (content) {
                content.style.display = 'block';
            }
        });
    });
}

// Export tab helper
window.initTabs = initTabs;

/**
 * Loading spinner HTML
 */
const LOADING_HTML = '<tr><td colspan="100" class="loading"><div class="spinner"></div><p>Loading...</p></td></tr>';
const EMPTY_HTML = '<tr><td colspan="100" class="empty-state"><i class="fas fa-inbox"></i><p>Tidak ada data</p></td></tr>';

window.LOADING_HTML = LOADING_HTML;
window.EMPTY_HTML = EMPTY_HTML;

console.log('✅ App.js loaded successfully');

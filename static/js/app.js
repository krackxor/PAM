/**
 * SUNTER Dashboard - Professional Main JavaScript
 * Global utilities with mobile-first optimizations
 */

const App = {
    /**
     * Rupiah & Number Formatting
     */
    formatRupiah: (amount) => {
        if (!amount && amount !== 0) return 'Rp 0';
        return new Intl.NumberFormat('id-ID', {
            style: 'currency', currency: 'IDR', minimumFractionDigits: 0
        }).format(amount);
    },

    formatNumber: (num) => {
        return num || num === 0 ? new Intl.NumberFormat('id-ID').format(num) : '0';
    },

    /**
     * Dynamic Toast Notification (Sync with CSS .toast)
     */
    toast: function(message, type = 'success', duration = 3000) {
        let container = document.querySelector('.toast-container');
        if (!container) {
            container = document.createElement('div');
            container.className = 'toast-container';
            document.body.appendChild(container);
        }

        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        
        const icons = {
            success: 'fa-check-circle',
            error: 'fa-times-circle',
            warning: 'fa-exclamation-triangle'
        };

        toast.innerHTML = `
            <i class="fas ${icons[type] || 'fa-info-circle'}"></i>
            <span>${message}</span>
        `;

        container.appendChild(toast);

        // Remove toast after duration
        setTimeout(() => {
            toast.style.opacity = '0';
            toast.style.transform = 'translateX(20px)';
            setTimeout(() => toast.remove(), 400);
        }, duration);
    },

    /**
     * UI Loaders
     */
    showLoading: function() {
        if (!document.getElementById('globalLoader')) {
            const loader = document.createElement('div');
            loader.id = 'globalLoader';
            loader.className = 'loading-overlay';
            loader.innerHTML = '<div class="spinner spinner-lg"></div>';
            document.body.appendChild(loader);
        }
    },

    hideLoading: function() {
        const loader = document.getElementById('globalLoader');
        if (loader) loader.remove();
    },

    /**
     * Drawer Control (Sidebar Mobile)
     */
    toggleDrawer: function(show = true) {
        const drawer = document.querySelector('.drawer');
        const overlay = document.querySelector('.drawer-overlay');
        if (drawer && overlay) {
            if (show) {
                drawer.classList.add('active');
                overlay.classList.add('active');
                document.body.style.overflow = 'hidden'; // Prevent scroll
            } else {
                drawer.classList.remove('active');
                overlay.classList.remove('active');
                document.body.style.overflow = '';
            }
        }
    },

    /**
     * Modal Control
     */
    showModal: function(modalId) {
        const modal = document.getElementById(modalId);
        if (modal) {
            modal.closest('.modal-overlay').classList.add('active');
            document.body.style.overflow = 'hidden';
        }
    },

    closeModals: function() {
        document.querySelectorAll('.modal-overlay').forEach(overlay => {
            overlay.classList.remove('active');
        });
        document.body.style.overflow = '';
    },

    /**
     * API Wrapper
     */
    api: async function(url, options = {}) {
        try {
            this.showLoading();
            const response = await fetch(url, {
                ...options,
                headers: { 'Content-Type': 'application/json', ...options.headers }
            });
            const data = await response.json();
            if (!response.ok) throw new Error(data.error || 'Server Error');
            return data;
        } catch (error) {
            this.toast(error.message, 'error');
            throw error;
        } finally {
            this.hideLoading();
        }
    }
};

// Bind to window
window.App = App;

/**
 * Event Listeners & Initialization
 */
document.addEventListener('DOMContentLoaded', () => {
    console.log('📱 Sunter Mobile Engine Ready');

    // 1. Navigation Active State
    const currentPath = window.location.pathname;
    document.querySelectorAll('.nav-item').forEach(link => {
        if (link.getAttribute('href') === currentPath) link.classList.add('active');
    });

    // 2. Global Click Handler for Drawer & Modals
    document.addEventListener('click', (e) => {
        // Toggle Drawer
        if (e.target.closest('.btn-drawer-open')) App.toggleDrawer(true);
        if (e.target.closest('.btn-drawer-close') || e.target.classList.contains('drawer-overlay')) {
            App.toggleDrawer(false);
        }

        // Close Modals on Overlay Click
        if (e.target.classList.contains('modal-overlay') || e.target.closest('.modal-close')) {
            App.closeModals();
        }
    });

    // 3. Tab Switching Logic
    document.querySelectorAll('.tab-item').forEach(tab => {
        tab.addEventListener('click', function() {
            const group = this.parentElement;
            group.querySelectorAll('.tab-item').forEach(t => t.classList.remove('active'));
            this.classList.add('active');
            
            // Trigger custom event for specific page logic
            const tabId = this.getAttribute('data-tab');
            document.dispatchEvent(new CustomEvent('tabChanged', { detail: tabId }));
        });
    });

    // 4. Keyboard Shortcuts
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') {
            App.toggleDrawer(false);
            App.closeModals();
        }
    });
});

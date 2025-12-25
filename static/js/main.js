/**
 * SUNTER DASHBOARD - Main JavaScript
 * Responsive utilities and interactions
 */

// ============================================
// UTILITY FUNCTIONS
// ============================================

/**
 * Format number as Rupiah
 */
function formatRupiah(amount) {
    return new Intl.NumberFormat('id-ID', {
        style: 'currency',
        currency: 'IDR',
        minimumFractionDigits: 0
    }).format(amount);
}

/**
 * Format number with thousand separator
 */
function formatNumber(num) {
    return new Intl.NumberFormat('id-ID').format(num);
}

/**
 * Debounce function for performance
 */
function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

/**
 * Check if device is mobile
 */
function isMobile() {
    return window.innerWidth <= 768;
}

/**
 * Check if device is touch-enabled
 */
function isTouchDevice() {
    return 'ontouchstart' in window || navigator.maxTouchPoints > 0;
}

// ============================================
// DRAWER/SIDEBAR
// ============================================

class Drawer {
    constructor(drawerId) {
        this.drawer = document.getElementById(drawerId);
        this.overlay = this.drawer?.previousElementSibling;
        this.isOpen = false;
        
        this.init();
    }
    
    init() {
        if (!this.drawer) return;
        
        // Close on overlay click
        this.overlay?.addEventListener('click', () => this.close());
        
        // Close on ESC key
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && this.isOpen) {
                this.close();
            }
        });
        
        // Close buttons
        const closeButtons = this.drawer.querySelectorAll('[data-drawer-close]');
        closeButtons.forEach(btn => {
            btn.addEventListener('click', () => this.close());
        });
    }
    
    open() {
        this.drawer?.classList.add('active');
        this.overlay?.classList.add('active');
        this.isOpen = true;
        document.body.style.overflow = 'hidden';
    }
    
    close() {
        this.drawer?.classList.remove('active');
        this.overlay?.classList.remove('active');
        this.isOpen = false;
        document.body.style.overflow = '';
    }
    
    toggle() {
        this.isOpen ? this.close() : this.open();
    }
}

// ============================================
// MODAL
// ============================================

class Modal {
    constructor(modalId) {
        this.modal = document.getElementById(modalId);
        this.overlay = this.modal?.closest('.modal-overlay');
        this.isOpen = false;
        
        this.init();
    }
    
    init() {
        if (!this.modal) return;
        
        // Close on overlay click
        this.overlay?.addEventListener('click', (e) => {
            if (e.target === this.overlay) {
                this.close();
            }
        });
        
        // Close on ESC key
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && this.isOpen) {
                this.close();
            }
        });
        
        // Close buttons
        const closeButtons = this.modal.querySelectorAll('[data-modal-close]');
        closeButtons.forEach(btn => {
            btn.addEventListener('click', () => this.close());
        });
    }
    
    open() {
        this.overlay?.classList.add('active');
        this.isOpen = true;
        document.body.style.overflow = 'hidden';
    }
    
    close() {
        this.overlay?.classList.remove('active');
        this.isOpen = false;
        document.body.style.overflow = '';
    }
    
    toggle() {
        this.isOpen ? this.close() : this.open();
    }
}

// ============================================
// TABS
// ============================================

class Tabs {
    constructor(tabsId) {
        this.tabs = document.getElementById(tabsId);
        this.tabItems = this.tabs?.querySelectorAll('.tab-item');
        this.tabPanels = this.tabs?.querySelectorAll('.tab-panel');
        
        this.init();
    }
    
    init() {
        if (!this.tabs) return;
        
        this.tabItems.forEach((tab, index) => {
            tab.addEventListener('click', () => {
                this.switchTab(index);
            });
        });
    }
    
    switchTab(index) {
        // Remove active class from all
        this.tabItems.forEach(tab => tab.classList.remove('active'));
        this.tabPanels.forEach(panel => panel.classList.remove('active'));
        
        // Add active to selected
        this.tabItems[index].classList.add('active');
        this.tabPanels[index].classList.add('active');
        
        // Scroll tab into view on mobile
        if (isMobile()) {
            this.tabItems[index].scrollIntoView({
                behavior: 'smooth',
                block: 'nearest',
                inline: 'center'
            });
        }
    }
}

// ============================================
// ACCORDION
// ============================================

class Accordion {
    constructor(accordionId) {
        this.accordion = document.getElementById(accordionId);
        this.items = this.accordion?.querySelectorAll('.accordion-item');
        
        this.init();
    }
    
    init() {
        if (!this.accordion) return;
        
        this.items.forEach(item => {
            const header = item.querySelector('.accordion-header');
            header.addEventListener('click', () => {
                this.toggle(item);
            });
        });
    }
    
    toggle(item) {
        const isActive = item.classList.contains('active');
        
        // Close all items (optional: remove to allow multiple open)
        this.items.forEach(i => i.classList.remove('active'));
        
        // Toggle clicked item
        if (!isActive) {
            item.classList.add('active');
        }
    }
}

// ============================================
// TOAST NOTIFICATIONS
// ============================================

class Toast {
    constructor() {
        this.container = this.createContainer();
    }
    
    createContainer() {
        let container = document.querySelector('.toast-container');
        if (!container) {
            container = document.createElement('div');
            container.className = 'toast-container';
            document.body.appendChild(container);
        }
        return container;
    }
    
    show(message, type = 'info', duration = 3000) {
        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        toast.innerHTML = `
            <div class="toast-icon">
                ${this.getIcon(type)}
            </div>
            <div class="toast-message">${message}</div>
        `;
        
        this.container.appendChild(toast);
        
        // Auto remove
        setTimeout(() => {
            toast.style.animation = 'slideOut 0.3s ease';
            setTimeout(() => toast.remove(), 300);
        }, duration);
    }
    
    getIcon(type) {
        const icons = {
            success: '✓',
            error: '✕',
            warning: '⚠',
            info: 'ℹ'
        };
        return icons[type] || icons.info;
    }
    
    success(message, duration) {
        this.show(message, 'success', duration);
    }
    
    error(message, duration) {
        this.show(message, 'error', duration);
    }
    
    warning(message, duration) {
        this.show(message, 'warning', duration);
    }
    
    info(message, duration) {
        this.show(message, 'info', duration);
    }
}

// Global toast instance
const toast = new Toast();

// ============================================
// LOADING OVERLAY
// ============================================

class Loading {
    constructor() {
        this.overlay = this.createOverlay();
    }
    
    createOverlay() {
        let overlay = document.querySelector('.loading-overlay');
        if (!overlay) {
            overlay = document.createElement('div');
            overlay.className = 'loading-overlay';
            overlay.innerHTML = '<div class="spinner spinner-lg"></div>';
            overlay.style.display = 'none';
            document.body.appendChild(overlay);
        }
        return overlay;
    }
    
    show() {
        this.overlay.style.display = 'flex';
    }
    
    hide() {
        this.overlay.style.display = 'none';
    }
}

// Global loading instance
const loading = new Loading();

// ============================================
// HAMBURGER MENU
// ============================================

function initHamburger() {
    const hamburger = document.querySelector('.hamburger');
    const drawer = new Drawer('main-drawer');
    
    hamburger?.addEventListener('click', () => {
        hamburger.classList.toggle('active');
        drawer.toggle();
    });
}

// ============================================
// RESPONSIVE TABLE
// ============================================

function makeTablesResponsive() {
    const tables = document.querySelectorAll('table:not(.table-container table)');
    
    tables.forEach(table => {
        if (!table.parentElement.classList.contains('table-container')) {
            const wrapper = document.createElement('div');
            wrapper.className = 'table-container';
            table.parentNode.insertBefore(wrapper, table);
            wrapper.appendChild(table);
        }
    });
}

// ============================================
// SMOOTH SCROLL
// ============================================

function initSmoothScroll() {
    document.querySelectorAll('a[href^="#"]').forEach(anchor => {
        anchor.addEventListener('click', function (e) {
            const href = this.getAttribute('href');
            if (href !== '#') {
                e.preventDefault();
                const target = document.querySelector(href);
                if (target) {
                    target.scrollIntoView({
                        behavior: 'smooth',
                        block: 'start'
                    });
                }
            }
        });
    });
}

// ============================================
// FORM VALIDATION
// ============================================

function validateForm(formId) {
    const form = document.getElementById(formId);
    if (!form) return false;
    
    let isValid = true;
    const requiredFields = form.querySelectorAll('[required]');
    
    requiredFields.forEach(field => {
        if (!field.value.trim()) {
            field.classList.add('is-invalid');
            isValid = false;
        } else {
            field.classList.remove('is-invalid');
        }
    });
    
    return isValid;
}

// ============================================
// API HELPER
// ============================================

async function apiRequest(url, options = {}) {
    try {
        loading.show();
        
        const response = await fetch(url, {
            headers: {
                'Content-Type': 'application/json',
                ...options.headers
            },
            ...options
        });
        
        const data = await response.json();
        
        if (!response.ok) {
            throw new Error(data.error || 'Request failed');
        }
        
        loading.hide();
        return data;
        
    } catch (error) {
        loading.hide();
        toast.error(error.message);
        throw error;
    }
}

// ============================================
// COPY TO CLIPBOARD
// ============================================

async function copyToClipboard(text) {
    try {
        await navigator.clipboard.writeText(text);
        toast.success('Copied to clipboard');
    } catch (err) {
        toast.error('Failed to copy');
    }
}

// ============================================
// DOWNLOAD FILE
// ============================================

function downloadFile(data, filename, type = 'text/csv') {
    const blob = new Blob([data], { type });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    window.URL.revokeObjectURL(url);
    a.remove();
    toast.success('File downloaded');
}

// ============================================
// RESIZE OBSERVER
// ============================================

function observeResize(callback) {
    const resizeObserver = new ResizeObserver(
        debounce((entries) => {
            callback(entries[0].contentRect);
        }, 150)
    );
    
    resizeObserver.observe(document.body);
    
    return resizeObserver;
}

// ============================================
// INITIALIZE ON LOAD
// ============================================

document.addEventListener('DOMContentLoaded', () => {
    // Initialize components
    initHamburger();
    makeTablesResponsive();
    initSmoothScroll();
    
    // Add touch class for touch devices
    if (isTouchDevice()) {
        document.body.classList.add('touch-device');
    }
    
    // Handle resize
    let resizeTimer;
    window.addEventListener('resize', () => {
        clearTimeout(resizeTimer);
        resizeTimer = setTimeout(() => {
            // Refresh on orientation change
            if (isMobile()) {
                makeTablesResponsive();
            }
        }, 250);
    });
    
    // Service Worker (optional - for PWA)
    if ('serviceWorker' in navigator) {
        navigator.serviceWorker.register('/sw.js').catch(() => {
            // Silent fail if no service worker
        });
    }
    
    console.log('✅ SUNTER Dashboard initialized');
});

// ============================================
// EXPORT FOR USE IN OTHER FILES
// ============================================

window.SunterDashboard = {
    formatRupiah,
    formatNumber,
    debounce,
    isMobile,
    isTouchDevice,
    Drawer,
    Modal,
    Tabs,
    Accordion,
    toast,
    loading,
    apiRequest,
    copyToClipboard,
    downloadFile,
    validateForm
};

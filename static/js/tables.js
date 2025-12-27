/**
 * SUNTER Dashboard - Professional Table Helpers
 * Optimized for Large Datasets and Mobile Viewports
 */

const TableHelpers = {
    /**
     * Render Table with Data & Formatters
     */
    render: function(tableId, data, columns, options = {}) {
        const table = document.getElementById(tableId);
        if (!table) return console.error('Table not found:', tableId);
        
        const tbody = table.querySelector('tbody');
        if (!tbody) return;
        
        tbody.innerHTML = '';
        
        // Handle Empty State
        if (!data || data.length === 0) {
            this.showEmpty(tableId, options.emptyMessage);
            return;
        }
        
        // Render Rows efficiently
        const fragment = document.createDocumentFragment();
        data.forEach((row, index) => {
            const tr = document.createElement('tr');
            
            if (options.onRowClick) {
                tr.classList.add('clickable-row');
                tr.addEventListener('click', () => options.onRowClick(row, index));
            }
            
            // Add Row Number if requested
            if (options.showRowNumber) {
                const tdNum = document.createElement('td');
                tdNum.textContent = index + 1;
                tdNum.classList.add('text-center', 'text-muted');
                tr.appendChild(tdNum);
            }

            columns.forEach(col => {
                const td = document.createElement('td');
                let value = this.getNestedValue(row, col.field);
                
                // Apply Formatter (e.g., formatRupiah)
                if (col.formatter) {
                    value = col.formatter(value, row, index);
                }
                
                if (typeof value === 'object' && value !== null) {
                    td.appendChild(value); // Support DOM elements
                } else {
                    td.innerHTML = value !== null && value !== undefined ? value : '-';
                }
                
                if (col.class) td.className = col.class;
                tr.appendChild(td);
            });
            
            fragment.appendChild(tr);
        });
        
        tbody.appendChild(fragment);
        
        // Post-render Features
        if (options.sortable) this.makeSortable(tableId);
    },

    /**
     * Smart Sorting - Supports Currency, Dates, and Numbers
     */
    sortTable: function(tableId, columnIndex) {
        const table = document.getElementById(tableId);
        const tbody = table.querySelector('tbody');
        const rows = Array.from(tbody.querySelectorAll('tr:not(.filter-empty)'));
        const header = table.querySelectorAll('thead th')[columnIndex];
        
        const isAsc = header.getAttribute('data-sort') !== 'asc';
        header.setAttribute('data-sort', isAsc ? 'asc' : 'desc');

        rows.sort((a, b) => {
            let valA = a.cells[columnIndex].textContent.trim();
            let valB = b.cells[columnIndex].textContent.trim();
            
            // Clean currency/formatted numbers: "Rp 1.000" -> 1000
            const numA = parseFloat(valA.replace(/[^\d.-]/g, ''));
            const numB = parseFloat(valB.replace(/[^\d.-]/g, ''));

            if (!isNaN(numA) && !isNaN(numB)) {
                return isAsc ? numA - numB : numB - numA;
            }
            return isAsc ? valA.localeCompare(valB) : valB.localeCompare(valA);
        });

        // Update UI Icons
        table.querySelectorAll('.sort-icon').forEach(i => i.className = 'fas fa-sort sort-icon');
        const icon = header.querySelector('.sort-icon');
        if (icon) icon.className = `fas fa-sort-${isAsc ? 'up' : 'down'} sort-icon active`;

        rows.forEach(row => tbody.appendChild(row));
    },

    /**
     * Fast Filtering with Debounce Support
     */
    filter: function(tableId, searchTerm) {
        const table = document.getElementById(tableId);
        const rows = table.querySelectorAll('tbody tr:not(.filter-empty)');
        const term = searchTerm.toLowerCase();
        let matchCount = 0;

        rows.forEach(row => {
            const isMatch = row.textContent.toLowerCase().includes(term);
            row.style.display = isMatch ? '' : 'none';
            if (isMatch) matchCount++;
        });

        // Toggle Empty Result Message
        const oldEmpty = table.querySelector('.filter-empty');
        if (oldEmpty) oldEmpty.remove();

        if (matchCount === 0) {
            const colCount = table.querySelectorAll('thead th').length;
            const emptyRow = document.createElement('tr');
            emptyRow.className = 'filter-empty';
            emptyRow.innerHTML = `<td colspan="${colCount}" class="empty-state text-center">
                <i class="fas fa-search mb-2"></i><p>Tidak ada hasil untuk "${searchTerm}"</p></td>`;
            table.querySelector('tbody').appendChild(emptyRow);
        }
    },

    /**
     * Professional States (Loading & Empty)
     */
    showLoading: function(tableId, rows = 5) {
        const table = document.getElementById(tableId);
        const tbody = table.querySelector('tbody');
        const colCount = table.querySelectorAll('thead th').length;
        
        tbody.innerHTML = '';
        for (let i = 0; i < rows; i++) {
            const tr = document.createElement('tr');
            tr.innerHTML = `<td colspan="${colCount}"><div class="skeleton" style="height:20px; width:100%"></div></td>`;
            tbody.appendChild(tr);
        }
    },

    showEmpty: function(tableId, message = 'Tidak ada data ditemukan') {
        const table = document.getElementById(tableId);
        const colCount = table.querySelectorAll('thead th').length;
        table.querySelector('tbody').innerHTML = `
            <tr><td colspan="${colCount}" class="empty-state text-center">
            <i class="fas fa-inbox mb-2"></i><p>${message}</p></td></tr>`;
    },

    /**
     * Utilities
     */
    getNestedValue: (obj, path) => {
        return path ? path.split('.').reduce((prev, curr) => prev ? prev[curr] : null, obj) : obj;
    },

    makeSortable: function(tableId) {
        const headers = document.querySelectorAll(`#${tableId} thead th[data-field]`);
        headers.forEach((th, idx) => {
            th.style.cursor = 'pointer';
            if (!th.querySelector('.sort-icon')) {
                th.innerHTML += ' <i class="fas fa-sort sort-icon" style="opacity:0.3; font-size:0.8em"></i>';
            }
            th.onclick = () => this.sortTable(tableId, idx);
        });
    }
};

window.TableHelpers = TableHelpers;
console.log('✅ Professional Tables.js Ready');

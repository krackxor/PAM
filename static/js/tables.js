/**
 * SUNTER Dashboard - Table Helpers
 * Helper functions for table manipulation
 */

const TableHelpers = {
    /**
     * Render table with data
     */
    render: function(tableId, data, columns, options = {}) {
        const table = document.getElementById(tableId);
        if (!table) {
            console.error('Table not found:', tableId);
            return;
        }
        
        const tbody = table.querySelector('tbody');
        if (!tbody) {
            console.error('Table body not found');
            return;
        }
        
        // Clear existing rows
        tbody.innerHTML = '';
        
        // Check if data is empty
        if (!data || data.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="${columns.length}" class="empty-state">
                        <i class="fas fa-inbox"></i>
                        <p>${options.emptyMessage || 'Tidak ada data'}</p>
                    </td>
                </tr>
            `;
            return;
        }
        
        // Render rows
        data.forEach((row, index) => {
            const tr = document.createElement('tr');
            
            // Add click handler if provided
            if (options.onRowClick) {
                tr.style.cursor = 'pointer';
                tr.addEventListener('click', () => options.onRowClick(row, index));
            }
            
            // Render columns
            columns.forEach(col => {
                const td = document.createElement('td');
                
                // Get value
                let value = this.getNestedValue(row, col.field);
                
                // Format value if formatter provided
                if (col.formatter) {
                    value = col.formatter(value, row, index);
                }
                
                // Set content
                if (typeof value === 'string' || typeof value === 'number') {
                    td.textContent = value;
                } else {
                    td.innerHTML = value;
                }
                
                // Add class if provided
                if (col.class) {
                    td.className = col.class;
                }
                
                tr.appendChild(td);
            });
            
            tbody.appendChild(tr);
        });
        
        // Add row number if needed
        if (options.showRowNumber) {
            this.addRowNumbers(tableId);
        }
        
        // Make sortable if needed
        if (options.sortable) {
            this.makeSortable(tableId);
        }
    },
    
    /**
     * Get nested object value by string path
     */
    getNestedValue: function(obj, path) {
        return path.split('.').reduce((prev, curr) => {
            return prev ? prev[curr] : null;
        }, obj);
    },
    
    /**
     * Add row numbers to table
     */
    addRowNumbers: function(tableId) {
        const table = document.getElementById(tableId);
        const rows = table.querySelectorAll('tbody tr');
        
        rows.forEach((row, index) => {
            const td = document.createElement('td');
            td.textContent = index + 1;
            td.style.textAlign = 'center';
            row.insertBefore(td, row.firstChild);
        });
        
        // Add header
        const thead = table.querySelector('thead tr');
        if (thead) {
            const th = document.createElement('th');
            th.textContent = 'No';
            th.style.textAlign = 'center';
            thead.insertBefore(th, thead.firstChild);
        }
    },
    
    /**
     * Make table sortable
     */
    makeSortable: function(tableId) {
        const table = document.getElementById(tableId);
        const headers = table.querySelectorAll('thead th');
        
        headers.forEach((header, index) => {
            header.style.cursor = 'pointer';
            header.innerHTML += ' <i class="fas fa-sort" style="font-size: 10px; opacity: 0.5;"></i>';
            
            header.addEventListener('click', () => {
                this.sortTable(tableId, index);
            });
        });
    },
    
    /**
     * Sort table by column
     */
    sortTable: function(tableId, columnIndex) {
        const table = document.getElementById(tableId);
        const tbody = table.querySelector('tbody');
        const rows = Array.from(tbody.querySelectorAll('tr'));
        
        // Toggle sort direction
        const header = table.querySelectorAll('thead th')[columnIndex];
        const currentDir = header.getAttribute('data-sort-dir') || 'asc';
        const newDir = currentDir === 'asc' ? 'desc' : 'asc';
        
        // Update all headers
        table.querySelectorAll('thead th').forEach(h => {
            h.removeAttribute('data-sort-dir');
            const icon = h.querySelector('i');
            if (icon) icon.className = 'fas fa-sort';
        });
        
        header.setAttribute('data-sort-dir', newDir);
        const icon = header.querySelector('i');
        if (icon) {
            icon.className = newDir === 'asc' ? 'fas fa-sort-up' : 'fas fa-sort-down';
        }
        
        // Sort rows
        rows.sort((a, b) => {
            const aText = a.cells[columnIndex].textContent.trim();
            const bText = b.cells[columnIndex].textContent.trim();
            
            // Try to parse as number
            const aNum = parseFloat(aText.replace(/[^0-9.-]/g, ''));
            const bNum = parseFloat(bText.replace(/[^0-9.-]/g, ''));
            
            if (!isNaN(aNum) && !isNaN(bNum)) {
                return newDir === 'asc' ? aNum - bNum : bNum - aNum;
            }
            
            // Sort as string
            return newDir === 'asc' 
                ? aText.localeCompare(bText)
                : bText.localeCompare(aText);
        });
        
        // Reattach rows
        rows.forEach(row => tbody.appendChild(row));
    },
    
    /**
     * Filter table rows
     */
    filter: function(tableId, searchTerm, columns = null) {
        const table = document.getElementById(tableId);
        const rows = table.querySelectorAll('tbody tr');
        
        searchTerm = searchTerm.toLowerCase();
        
        rows.forEach(row => {
            let match = false;
            const cells = Array.from(row.cells);
            
            // If columns specified, search only those columns
            const cellsToSearch = columns 
                ? cells.filter((_, i) => columns.includes(i))
                : cells;
            
            cellsToSearch.forEach(cell => {
                if (cell.textContent.toLowerCase().includes(searchTerm)) {
                    match = true;
                }
            });
            
            row.style.display = match ? '' : 'none';
        });
        
        // Show empty state if no matches
        const visibleRows = Array.from(rows).filter(r => r.style.display !== 'none');
        if (visibleRows.length === 0) {
            const tbody = table.querySelector('tbody');
            const emptyRow = document.createElement('tr');
            emptyRow.className = 'filter-empty';
            emptyRow.innerHTML = `
                <td colspan="100" class="empty-state">
                    <i class="fas fa-search"></i>
                    <p>Tidak ada hasil untuk "${searchTerm}"</p>
                </td>
            `;
            tbody.appendChild(emptyRow);
        } else {
            // Remove empty state if exists
            const emptyRow = table.querySelector('.filter-empty');
            if (emptyRow) emptyRow.remove();
        }
    },
    
    /**
     * Paginate table
     */
    paginate: function(tableId, page = 1, perPage = 10) {
        const table = document.getElementById(tableId);
        const rows = Array.from(table.querySelectorAll('tbody tr'));
        
        const start = (page - 1) * perPage;
        const end = start + perPage;
        
        rows.forEach((row, index) => {
            row.style.display = (index >= start && index < end) ? '' : 'none';
        });
        
        // Return pagination info
        return {
            page: page,
            perPage: perPage,
            total: rows.length,
            totalPages: Math.ceil(rows.length / perPage)
        };
    },
    
    /**
     * Export table to CSV
     */
    exportToCSV: function(tableId, filename = 'export.csv') {
        const table = document.getElementById(tableId);
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
        
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
        
        if (window.App) {
            window.App.toast('Export berhasil', 'success');
        }
    },
    
    /**
     * Show loading state
     */
    showLoading: function(tableId) {
        const table = document.getElementById(tableId);
        const tbody = table.querySelector('tbody');
        const colSpan = table.querySelectorAll('thead th').length;
        
        tbody.innerHTML = `
            <tr>
                <td colspan="${colSpan}" class="loading">
                    <div class="spinner"></div>
                    <p>Loading...</p>
                </td>
            </tr>
        `;
    },
    
    /**
     * Show empty state
     */
    showEmpty: function(tableId, message = 'Tidak ada data') {
        const table = document.getElementById(tableId);
        const tbody = table.querySelector('tbody');
        const colSpan = table.querySelectorAll('thead th').length;
        
        tbody.innerHTML = `
            <tr>
                <td colspan="${colSpan}" class="empty-state">
                    <i class="fas fa-inbox"></i>
                    <p>${message}</p>
                </td>
            </tr>
        `;
    }
};

// Make TableHelpers global
window.TableHelpers = TableHelpers;

console.log('✅ Tables.js loaded successfully');

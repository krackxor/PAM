{% extends "base.html" %}

{% block title %}Laporan Koleksi & Analisis Kustom{% endblock %}

{% block custom_styles %}
/* Custom CSS untuk Tampilan Report/Detail yang Responsif */
.report-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); 
    gap: 15px;
    margin-bottom: 30px;
}
.summary-card {
    background-color: var(--card-bg);
    padding: 15px;
    border-radius: var(--border-radius);
    border-left: 5px solid; 
    box-shadow: var(--shadow-light);
}
.summary-card h4 { margin: 0 0 5px 0; font-size: 0.9em; text-transform: uppercase; color: #6c757d; }
.summary-card p { margin: 0; font-size: 1.3em; font-weight: bold; }

/* Wrapper scroll untuk responsivitas tabel lebar */
.report-table-wrapper { 
    overflow-x: auto; 
    margin-top: 15px; 
}

/* Style Tabel Report */
.report-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 14px;
    min-width: 1000px; /* Minimal width untuk scroll horizontal yang jelas */
}
.report-table th, .report-table td {
    border: 1px solid #ddd;
    padding: 8px 6px; 
    white-space: nowrap; 
    text-align: right;
}
.report-table th {
    background-color: #f2f2f2;
    text-align: center;
}
.report-table th[rowspan="2"] {
    min-width: 80px; /* Tambahkan min-width untuk header Rayon/PCEZ */
}
.report-table td:first-child, .report-table td:nth-child(2) {
    text-align: left;
}
.grand-total-row td {
    font-weight: bold;
    background-color: #fff3cd;
}

/* Styling untuk kelompok tabel kustom (AB Sunter) */
.breakdown-group {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); /* 3 kolom yang responsif */
    gap: 20px;
    margin-top: 15px;
    margin-bottom: 30px;
}
.breakdown-table-container {
    padding: 15px;
    border: 1px solid #e0e0e0;
    border-radius: var(--border-radius);
    box-shadow: var(--shadow-light);
}
.breakdown-table-container table {
    min-width: auto; /* Hilangkan min-width untuk breakdown tables */
}
.breakdown-table-container table th:nth-child(1), 
.breakdown-table-container table td:nth-child(1) {
    text-align: left !important;
}

/* Filter Group Responsiveness */
.filter-input-group {
    display: flex;
    gap: 10px;
    margin-top: 30px;
    margin-bottom: 20px;
}
#filterInput {
    flex-grow: 1;
    padding: 12px;
    border: 1px solid #ced4da;
    border-radius: 4px;
}

/* ========================================================== */
/* MOBILE LAYOUT FIXES (< 600px) */
/* ========================================================== */
@media screen and (max-width: 600px) {
    
    /* General Grid */
    .report-grid {
        grid-template-columns: 1fr;
    }
    .breakdown-group {
        grid-template-columns: 1fr;
    }
    
    /* Filter Group: Tumpuk input dan tombol */
    .filter-input-group {
        flex-direction: column;
        gap: 15px;
    }
    #filterInput, #searchBtn {
        width: 100%;
        box-sizing: border-box;
    }
    
    /* Tables: Keep horizontal scroll but make it more compact/readable */
    /* Berlaku untuk tabel utama report dan grouping MB */
    .report-table, .collection-table {
        min-width: 700px; /* Pertahankan min-width agar horizontal scroll berfungsi */
        width: auto;
    }
    .report-table th, .report-table td, 
    .collection-table th, .collection-table td {
        padding: 6px 4px; /* Kompakkan padding */
        font-size: 11px;
    }
    
    /* Tabel Tarif Breakdown & Detail Listing MB: Implement Card View */
    
    /* Hapus bayangan/border wrapper agar tampilan card lebih bersih */
    #tarifBreakdownArea .report-table-wrapper,
    #customMBReportArea .report-table-wrapper {
        border: none;
        box-shadow: none;
    }
    
    /* Card View Logic (Semua tabel di halaman ini) */
    #tarifBreakdownArea .report-table,
    #customMBReportArea .report-table[style*="min-width: 600px"],
    #customMBReportArea .report-table[style*="min-width: 450px"],
    #customReportArea .report-table[style*="min-width: 100%"] { 
        min-width: 100% !important;
    }
    
    #tarifBreakdownArea .report-table thead,
    #customMBReportArea .report-table[style*="min-width: 600px"] thead,
    #customMBReportArea .report-table[style*="min-width: 450px"] thead,
    #customReportArea .report-table[style*="min-width: 100%"] thead {
        display: none;
    }
    #tarifBreakdownArea .report-table tr,
    #customMBReportArea .report-table[style*="min-width: 600px"] tr,
    #customMBReportArea .report-table[style*="min-width: 450px"] tr,
    #customReportArea .report-table[style*="min-width: 100%"] tr {
        display: block;
        margin-bottom: 10px;
        border: 1px solid #ddd;
        border-radius: var(--border-radius);
    }
    #tarifBreakdownArea .report-table td,
    #customMBReportArea .report-table[style*="min-width: 600px"] td,
    #customMBReportArea .report-table[style*="min-width: 450px"] td,
    #customReportArea .report-table[style*="min-width: 100%"] td {
        display: flex;
        justify-content: space-between;
        padding: 8px 12px;
        border: none;
        border-bottom: 1px solid #eee;
        text-align: right;
        font-size: 13px; /* Ukuran font standar untuk konten card */
    }
    #tarifBreakdownArea .report-table td:before,
    #customMBReportArea .report-table[style*="min-width: 600px"] td:before,
    #customMBReportArea .report-table[style*="min-width: 450px"] td:before,
    #customReportArea .report-table[style*="min-width: 100%"] td:before {
        content: attr(data-label);
        font-weight: bold;
        text-align: left;
        width: 50%;
        color: var(--text-color);
        padding-right: 10px;
        white-space: nowrap;
    }
    #tarifBreakdownArea .grand-total-row td,
    #customMBReportArea .grand-total-row td,
    #customReportArea .grand-total-row td {
        background-color: #f8f8f8;
    }
    
}
{% endblock %}

{% block content %}
    <h2 style="text-align: left; border-bottom: none; margin-bottom: 20px; color: var(--primary-color);">
        <i class="fas fa-chart-bar" style="margin-right: 10px;"></i> Laporan Koleksi & Analisis Kustom
    </h2>
    
    <h3 style="margin-top: 40px; color: #dc3545; border-top: 1px solid #dc3545; padding-top: 20px;">
        <i class="fas fa-layer-group"></i> Modul Analisis Grup MC: Piutang Pelanggan (AB Sunter)
    </h3>
    
    <button id="fetchCustomReportBtn" class="btn-primary" style="background-color: #dc3545; color: white; margin-bottom: 20px;">
        <i class="fas fa-sync"></i> Muat Data Grup MC & Breakdown Tarif
    </button>

    <div id="customReportArea">
        <p class="no-results">Tekan tombol di atas untuk memuat laporan agregasi Piutang (MC) untuk area AB Sunter.</p>
    </div>
    
    <h3 style="margin-top: 40px; color: #17a2b8; border-top: 1px solid #17a2b8; padding-top: 20px;">
        <i class="fas fa-money-check-alt"></i> Modul Analisis Grup MB: Koleksi Pembayaran (AB Sunter)
    </h3>
    
    <button id="fetchCustomMBReportBtn" class="btn-primary" style="background-color: #17a2b8; color: white; margin-bottom: 20px;">
        <i class="fas fa-sync"></i> Muat Laporan Koleksi AB Sunter
    </button>

    <div id="customMBReportArea">
        <p class="no-results">Tekan tombol di atas untuk memuat laporan agregasi Koleksi (MB) untuk area AB Sunter.</p>
    </div>

    <h3 style="margin-top: 40px; color: #dc3545; border-top: 1px solid #dc3545; padding-top: 20px;">
        <i class="fas fa-list-ol"></i> BREAKDOWN MC: Distribusi Pelanggan Berdasarkan TARIF
    </h3>
    <div id="tarifBreakdownArea">
        <p class="no-results">Tekan "Muat Data Grup MC & Breakdown Tarif" di atas untuk melihat hasil agregasi Tarif.</p>
    </div>

    <script>
        // Fungsi pembantu
        function formatRupiah(number) {
            return 'Rp ' + (parseFloat(number) || 0).toLocaleString('id-ID', { maximumFractionDigits: 0 });
        }
        
        function formatNumber(number) {
            return (parseFloat(number) || 0).toLocaleString('id-ID', { maximumFractionDigits: 0 });
        }

        function formatPercent(number) {
            return (parseFloat(number) || 0).toFixed(2) + '%';
        }
        
        // MC Constants
        const fetchCustomBtn = document.getElementById('fetchCustomReportBtn'); 
        const customReportArea = document.getElementById('customReportArea');
        const tarifBreakdownArea = document.getElementById('tarifBreakdownArea');
        
        // MB Constants
        const fetchCustomMBBtn = document.getElementById('fetchCustomMBReportBtn');
        const customMBReportArea = document.getElementById('customMBReportArea');
        
        // --- FUNGSI MC: RENDER BREAKDOWN TARIF DINAMIS ---
        function renderTarifBreakdownSection(tarifData) {
            const area = document.getElementById('tarifBreakdownArea');
            area.innerHTML = ''; // Clear existing content

            function createTarifTable(title, dataArray) {
                let totalPelanggan = dataArray.reduce((sum, item) => sum + item.CountOfNOMEN, 0);

                let tableHTML = `
                    <div class="breakdown-table-container">
                        <h5 style="margin-top: 0; color: #17a2b8;">${title} (Total Nomen: ${formatNumber(totalPelanggan)})</h5>
                        <div class="report-table-wrapper" style="margin-top: 10px;">
                            <table class="report-table" style="min-width: 100%; font-size: 13px;">
                                <thead>
                                    <tr style="background-color: #f0f8ff;">
                                        <th style="text-align: left;" data-label="TARIF">TARIF</th>
                                        <th data-label="Nomen">Nomen Count</th>
                                        <th data-label="Nominal">Nominal (Rp)</th>
                                        <th data-label="Persentase">Persentase (%)</th>
                                    </tr>
                                </thead>
                                <tbody>
                `;
                
                dataArray.forEach(item => {
                    const percentage = (item.CountOfNOMEN / totalPelanggan) * 100;
                    tableHTML += `
                        <tr>
                            <td data-label="TARIF" style="text-align: left;">${item.TARIF || 'N/A'}</td>
                            <td data-label="Nomen">${formatNumber(item.CountOfNOMEN)}</td>
                            <td data-label="Nominal">${formatRupiah(item.SumOfNOMINAL)}</td>
                            <td data-label="Persentase">${formatPercent(percentage)}</td>
                        </tr>
                    `;
                });
                
                tableHTML += `
                    <tr class="grand-total-row">
                        <td data-label="TOTAL" style="text-align: left;">TOTAL</td>
                        <td data-label="Total Nomen">${formatNumber(totalPelanggan)}</td>
                        <td data-label="Total Nominal">${formatRupiah(dataArray.reduce((sum, item) => sum + item.SumOfNOMINAL, 0))}</td>
                        <td data-label="Persentase">100.00%</td>
                    </tr>
                `;

                tableHTML += `
                                </tbody>
                            </table>
                        </div>
                    </div>
                `;
                return tableHTML;
            }

            let breakdownHTML = `
                <div class="breakdown-group">
                    ${createTarifTable('AB SUNTER (Total R34 + R35)', tarifData.TOTAL_34_35)}
                    ${createTarifTable('Rayon 34', tarifData['34'])}
                    ${createTarifTable('Rayon 35', tarifData['35'])}
                </div>
            `;
            area.innerHTML = breakdownHTML;
        }

        // --- FUNGSI MC: GROUPING KUSTOM ---
        async function fetchCustomReport() {
            customReportArea.innerHTML = '<p class="no-results"><i class="fas fa-spinner fa-spin"></i> Menghubungkan ke MongoDB untuk Laporan Agregasi MC...</p>';
            
            try {
                const response = await fetch('{{ url_for("analyze_mc_grouping_api") }}'); 
                const data = await response.json();
                
                if (response.status === 401) {
                    window.location.href = '{{ url_for("login") }}';
                    return;
                }
                
                if (data.status !== 'success') {
                     customReportArea.innerHTML = `<p class="no-results" style="color: red;">❌ Gagal memuat laporan: ${data.message || 'Tidak ada data kustom ditemukan.'}</p>`;
                     tarifBreakdownArea.innerHTML = `<p class="no-results" style="color: red;">Data Tarif Gagal dimuat.</p>`;
                     return;
                }
                
                // NEW: RENDER TARIF BREAKDOWN SECTION (Integrated with MC button)
                renderTarifBreakdownSection(data.breakdowns.TARIF);
                
                renderCustomReportTable(data);

            } catch (error) {
                customReportArea.innerHTML = '<p class="no-results" style="color: red;">Gagal mengambil data laporan kustom dari server.</p>';
                tarifBreakdownArea.innerHTML = `<p class="no-results" style="color: red;">Data Tarif Gagal dimuat.</p>`;
                console.error('Custom Report Error:', error);
            }
        }
        
        function renderCustomReportTable(data) {
            const customArea = document.getElementById('customReportArea');
            if (data.status !== 'success') {
                 customArea.innerHTML = `<p class="no-results" style="color: red;">❌ ${data.message || 'Gagal memuat laporan kustom.'}</p>`;
                 return;
            }

            const totals = data.totals;
            const breakdowns = data.breakdowns;

            // 1. RENDER RINGKASAN TOTAL MC
            let totalSummaryHTML = `
                <h4 style="color: #dc3545; margin-bottom: 15px;">Ringkasan Nomen, Nominal, dan Kubikasi (Reguler - MC)</h4>
                <div class="report-table-wrapper" style="max-width: 800px; margin-left: auto; margin-right: auto;">
                    <table class="report-table" style="min-width: 100%;">
                        <thead>
                            <tr style="background-color: #f8d7da; color: #721c24;">
                                <th style="text-align: left;">AREA</th>
                                <th>Nomen Count</th>
                                <th>Total Nominal</th>
                                <th>Total Kubik (m³)</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr>
                                <td data-label="AREA" style="text-align: left; font-weight: bold;">AB SUNTER (Total R34 + R35)</td>
                                <td data-label="Nomen Count">${formatNumber(totals.TOTAL_34_35.CountOfNOMEN)}</td>
                                <td data-label="Total Nominal">${formatRupiah(totals.TOTAL_34_35.SumOfNOMINAL)}</td>
                                <td data-label="Total Kubik">${formatNumber(totals.TOTAL_34_35.SumOfKUBIK)}</td>
                            </tr>
                            <tr>
                                <td data-label="AREA" style="text-align: left; font-weight: 600;">Rayon 34</td>
                                <td data-label="Nomen Count">${formatNumber(totals[34].CountOfNOMEN)}</td>
                                <td data-label="Total Nominal">${formatRupiah(totals[34].SumOfNOMINAL)}</td>
                                <td data-label="Total Kubik">${formatNumber(totals[34].SumOfKUBIK)}</td>
                            </tr>
                            <tr>
                                <td data-label="AREA" style="text-align: left; font-weight: 600;">Rayon 35</td>
                                <td data-label="Nomen Count">${formatNumber(totals[35].CountOfNOMEN)}</td>
                                <td data-label="Total Nominal">${formatRupiah(totals[35].SumOfNOMINAL)}</td>
                                <td data-label="Total Kubik">${formatNumber(totals[35].SumOfKUBIK)}</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
                
                <h4 style="color: var(--primary-color); margin-top: 30px;">Detail Breakdown Per Dimensi (MC)</h4>
            `;

            // 2. RENDER BREAKDOWN PER DIMENSI MC
            function createSingleBreakdownTable(title, dataArray, dimensionKey) {
                let totalNomenCount = 0;
                let totalNominal = 0;
                
                dataArray.forEach(item => {
                    totalNomenCount += item.CountOfNOMEN || 0;
                    totalNominal += item.SumOfNOMINAL || 0;
                });

                let tableHTML = `
                    <div class="breakdown-table-container">
                        <h5 style="margin-top: 0; color: #007bff;">${title}</h5>
                        <div class="report-table-wrapper" style="margin-top: 10px;">
                            <table class="report-table" style="min-width: 100%;">
                                <thead>
                                    <tr style="background-color: #f0f8ff;">
                                        <th style="text-align: left; width: 40%;" data-label="Dimensi">${dimensionKey.replace(/_/g, ' ').toUpperCase()}</th>
                                        <th data-label="Nomen">Nomen Count</th>
                                        <th data-label="Nominal">Nominal</th>
                                    </tr>
                                </thead>
                                <tbody>
                `;
                
                dataArray.forEach(item => {
                    const dimValue = item[dimensionKey] || 'N/A';
                    const nomenCount = formatNumber(item.CountOfNOMEN);
                    const nominal = formatRupiah(item.SumOfNOMINAL);
                    
                    tableHTML += `
                        <tr>
                            <td data-label="${dimensionKey.toUpperCase()}" style="text-align: left;">${dimValue}</td>
                            <td data-label="Nomen Count">${nomenCount}</td>
                            <td data-label="Nominal">${nominal}</td>
                        </tr>
                    `;
                });
                
                tableHTML += `
                    <tr class="grand-total-row">
                        <td data-label="TOTAL" style="text-align: left;">GRAND TOTAL</td>
                        <td data-label="Total Nomen">${formatNumber(totalNomenCount)}</td>
                        <td data-label="Total Nominal">${formatRupiah(totalNominal)}</td>
                    </tr>
                `;

                tableHTML += `
                                </tbody>
                            </table>
                        </div>
                    </div>
                `;
                return tableHTML;
            }

            function renderDimensionGroup(dimensionName, dimensionKey, breakdownData) {
                let groupHTML = `
                    <h5 style="color: #007bff; margin-top: 25px; border-bottom: 1px dashed #ddd; padding-bottom: 5px;">Breakdown Berdasarkan: ${dimensionName}</h5>
                    <div class="breakdown-group">
                        ${createSingleBreakdownTable(`AB SUNTER (Total)`, breakdownData.TOTAL_34_35, dimensionKey)}
                        ${createSingleBreakdownTable(`Rayon 34`, breakdownData[34], dimensionKey)}
                        ${createSingleBreakdownTable(`Rayon 35`, breakdownData[35], dimensionKey)}
                    </div>
                `;
                return groupHTML;
            }

            let breakdownHTML = '';
            // TARIF breakdown dihapus karena sudah dipindahkan ke section terpisah
            breakdownHTML += renderDimensionGroup('Merek Meter', 'MERK', breakdowns.MERK);
            breakdownHTML += renderDimensionGroup('Metode Baca', 'READ_METHOD', breakdowns.READ_METHOD);

            customArea.innerHTML = totalSummaryHTML + breakdownHTML;
        }

        // --- FUNGSI MB: GROUPING KUSTOM (FINAL) ---
        async function fetchCustomMBReport() {
            customMBReportArea.innerHTML = '<p class="no-results"><i class="fas fa-spinner fa-spin"></i> Menghubungkan ke MongoDB untuk Laporan Agregasi MB...</p>';
            
            try {
                const response = await fetch('{{ url_for("analyze_mb_grouping_api") }}'); 
                const data = await response.json();
                
                if (response.status === 401) {
                    window.location.href = '{{ url_for("login") }}';
                    return;
                }
                
                if (data.status !== 'success') {
                     customMBReportArea.innerHTML = `<p class="no-results" style="color: red;">❌ Gagal memuat laporan: ${data.message || 'Tidak ada data koleksi AB Sunter ditemukan.'}</p>`;
                     return;
                }
                
                renderCustomMBReport(data);

            } catch (error) {
                customMBReportArea.innerHTML = '<p class="no-results" style="color: red;">Gagal mengambil data koleksi MB dari server.</p>';
                console.error('Custom MB Report Error:', error);
            }
        }
        
        function renderCustomMBReport(data) {
            const area = document.getElementById('customMBReportArea');
            const r34 = data.rayon_34;
            const r35 = data.rayon_35;
            const abSunter = data.ab_sunter;
            
            // --- Helper function to create one of the three summary tables ---
            function createSummaryTable(title, metricKey) {
                let nomenKey, nominalKey;
                if (metricKey === 'total') {
                    nomenKey = 'CountOfNOMEN';
                    nominalKey = 'SumOfNOMINAL';
                } else if (metricKey === 'undue') {
                    nomenKey = 'CountOfNOMEN_UNDUE';
                    nominalKey = 'SumOfNOMINAL_UNDUE';
                } else { // tunggakan
                    nomenKey = 'CountOfNOMEN_TUNGGAKAN';
                    nominalKey = 'SumOfNOMINAL_TUNGGAKAN';
                }

                let tableHTML = `
                    <div class="breakdown-table-container">
                        <h5 style="margin-top: 0; color: ${metricKey === 'total' ? '#007bff' : metricKey === 'undue' ? '#28a745' : '#dc3545'};">
                            ${title}
                        </h5>
                        <div class="report-table-wrapper">
                            <table class="report-table" style="min-width: 100%; font-size: 13px;">
                                <thead>
                                    <tr style="background-color: #f0f8ff;">
                                        <th style="text-align: left;" data-label="Area">AREA</th>
                                        <th data-label="Nomen">NOMEN (Count)</th>
                                        <th data-label="Nominal">NOMINAL (Rp)</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    <tr>
                                        <td data-label="Area" style="text-align: left; font-weight: bold;">AB SUNTER (Total)</td>
                                        <td data-label="Nomen">${formatNumber(abSunter[metricKey][nomenKey])}</td>
                                        <td data-label="Nominal">${formatRupiah(abSunter[metricKey][nominalKey])}</td>
                                    </tr>
                                    <tr>
                                        <td data-label="Area" style="text-align: left; font-weight: 600;">Rayon 34</td>
                                        <td data-label="Nomen">${formatNumber(r34[metricKey][nomenKey])}</td>
                                        <td data-label="Nominal">${formatRupiah(r34[metricKey][nominalKey])}</td>
                                    </tr>
                                    <tr>
                                        <td data-label="Area" style="text-align: left; font-weight: 600;">Rayon 35</td>
                                        <td data-label="Nomen">${formatNumber(r35[metricKey][nomenKey])}</td>
                                        <td data-label="Nominal">${formatRupiah(r35[metricKey][nominalKey])}</td>
                                    </tr>
                                </tbody>
                            </table>
                        </div>
                    </div>
                `;
                return tableHTML;
            }
            
            // --- Helper function to create the daily summary table (TABEL 4) ---
            function createDailySummaryTable(title, detailsArray) {
                
                if (detailsArray.length === 0) {
                    return `
                        <h5 style="margin-top: 25px; color: #6c757d;">${title} (0 Hari)</h5>
                        <p class="no-results" style="font-size: 0.9em;">Tidak ada ringkasan transaksi harian ditemukan untuk area ini.</p>
                    `;
                }
                
                let totalNominal = 0;
                let totalNomen = 0;
                detailsArray.forEach(item => {
                    totalNominal += item.SumOfNOMINAL || 0;
                    totalNomen += item.CountOfNOMEN || 0;
                });

                let tableHTML = `
                    <div class="breakdown-table-container">
                        <h5 style="margin-top: 0; color: #007bff;">${title} (${detailsArray.length} Hari Transaksi Terbaru)</h5>
                        <div class="report-table-wrapper" style="margin-top: 10px;">
                            <table class="report-table" style="min-width: 450px; font-size: 13px;">
                                <thead>
                                    <tr style="background-color: #f0f8ff;">
                                        <th data-label="TGL BAYAR">TGL BAYAR</th>
                                        <th data-label="Nomen">NOMEN (Count)</th>
                                        <th data-label="Nominal">NOMINAL (Rp)</th>
                                    </tr>
                                </thead>
                                <tbody>
                `;
                
                detailsArray.forEach(item => {
                    // Reformat date from YYYY-MM-DD to DD/MM/YYYY for display
                    let displayDate = item.TGL_BAYAR;
                    if (displayDate && displayDate.length >= 10 && displayDate.includes('-')) {
                        const parts = displayDate.split('-');
                        displayDate = `${parts[2]}/${parts[1]}/${parts[0]}`;
                    }

                    tableHTML += `
                        <tr>
                            <td data-label="TGL BAYAR">${displayDate}</td>
                            <td data-label="Nomen">${formatNumber(item.CountOfNOMEN)}</td>
                            <td data-label="Nominal">${formatRupiah(item.SumOfNOMINAL)}</td>
                        </tr>
                    `;
                });
                
                tableHTML += `
                    <tr class="grand-total-row">
                        <td data-label="TOTAL" style="text-align: left;">TOTAL PERIODE</td>
                        <td data-label="Total Nomen">${formatNumber(totalNomen)}</td>
                        <td data-label="Total Nominal">${formatRupiah(totalNominal)}</td>
                    </tr>
                `;

                tableHTML += `
                        </tbody>
                    </table>
                </div>
                </div>
                `;
                return tableHTML;
            }


            // 1. RINGKASAN KOLEKSI (3 Tabel Terpisah)
            let summaryHTML = `
                <h4 style="color: #17a2b8; margin-bottom: 15px;">Ringkasan Koleksi Berdasarkan Status Pembayaran</h4>
                
                <h5 style="color: #007bff; margin-top: 25px; border-bottom: 1px dashed #ddd; padding-bottom: 5px;">TABEL 1: TOTAL KOLEKSI (UNDUE + TUNGGAKAN)</h5>
                <div class="breakdown-group">
                    ${createSummaryTable('Total Koleksi (MB)', 'total')}
                </div>
                
                <h5 style="color: #28a745; margin-top: 25px; border-bottom: 1px dashed #ddd; padding-bottom: 5px;">TABEL 2: KOLEKSI UNDUE (BULAN BERJALAN)</h5>
                <div class="breakdown-group">
                    ${createSummaryTable('Koleksi Undue', 'undue')}
                </div>
                
                <h5 style="color: #dc3545; margin-top: 25px; border-bottom: 1px dashed #ddd; padding-bottom: 5px;">TABEL 3: KOLEKSI TUNGGAKAN (PIUTANG LAMA)</h5>
                <div class="breakdown-group">
                    ${createSummaryTable('Koleksi Tunggakan', 'tunggakan')}
                </div>
            `;
            
            // 2. DETAIL KOLEKSI HARIAN (3 Tabel Terpisah)
            let detailsHTML = `
                <h4 style="color: var(--primary-color); margin-top: 30px;">TABEL 4: RINGKASAN KOLEKSI HARIAN BERDASARKAN TANGGAL</h4>
                
                <div class="breakdown-group">
                    ${createDailySummaryTable('AB SUNTER (Total R34 + R35)', abSunter.details)}
                    ${createDailySummaryTable('Rayon 34', r34.details)}
                    ${createDailySummaryTable('Rayon 35', r35.details)}
                </div>
            `;

            area.innerHTML = summaryHTML + detailsHTML;
        }

        // Event Listeners
        fetchCustomBtn.addEventListener('click', fetchCustomReport);
        fetchCustomMBBtn.addEventListener('click', fetchCustomMBReport); 
        
        // Muat semua bagian saat halaman dimuat
        document.addEventListener('DOMContentLoaded', () => {
             // fetchCustomMBReport() dihapus dari sini agar tidak auto-load
        });

    </script>
{% endblock %}

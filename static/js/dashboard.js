/**
 * SUNTER DASHBOARD - MAIN LOGIC
 * Mengelola pembaruan data real-time, grafik, dan tabel.
 */

let chartRayon, chartTren, tableColl;

// --- FORMATTER HELPERS ---
const formatRp = (val) => {
    return 'Rp ' + new Intl.NumberFormat('id-ID', {
        maximumFractionDigits: 0
    }).format(val || 0);
};

const formatNum = (val) => {
    return new Intl.NumberFormat('id-ID').format(val || 0);
};

// --- CORE FUNCTIONS ---

/**
 * Update semua angka KPI di halaman Ringkasan
 */
function updateKPI() {
    $.get('/api/kpi_data', function(d) {
        // Total Pelanggan
        $('#kpi-total-pelanggan').text(formatNum(d.total_pelanggan));
        
        // Collection Rate
        $('#kpi-rate').text(d.collection_rate + '%');
        $('#kpi-rate-bar').css('width', d.collection_rate + '%').text(d.collection_rate + '%');
        
        // Tunggakan (Ardebt)
        $('#kpi-tunggakan-total').text(formatRp(d.tunggakan.total_nominal));
        
        // Analisa Target vs Realisasi
        $('#kpi-target-total').text(formatRp(d.target.total_nominal));
        $('#kpi-target-nomen-total').text(formatNum(d.target.total_nomen) + ' Lbr');
        
        $('#kpi-coll-total').text(formatRp(d.collection.total_nominal));
        $('#kpi-coll-nomen-total').text(formatNum(d.collection.total_nomen) + ' Lbr');
        
        $('#kpi-target-belum').text(formatRp(d.target.belum_bayar_nominal));
        $('#kpi-target-nomen-belum').text(formatNum(d.target.belum_bayar_nomen) + ' Lbr');
        
        // Label Periode di Header
        $('#labelPeriode').html('<i class="fas fa-calendar-alt me-2"></i>Periode: ' + d.periode);
    }).fail(function() {
        console.error("Gagal memuat data KPI.");
    });
}

/**
 * Mengisi Tabel Ringkasan Wilayah di Tab Collection
 * (AB Sunter, 34, 35)
 */
function loadSummaryTable() {
    $.get('/api/collection_summary_table', function(data) {
        const tbody = $('#summaryTable tbody');
        tbody.empty();

        data.forEach(item => {
            const d = item.data;
            // Hitung % Capaian
            const plbr = d.target_nomen > 0 ? (d.realisasi_nomen / d.target_nomen * 100).toFixed(1) : 0;
            const prp = d.target_nominal > 0 ? (d.realisasi_nominal / d.target_nominal * 100).toFixed(1) : 0;

            tbody.append(`
                <tr class="${item.class}">
                    <td class="text-start ps-4">${item.kategori}</td>
                    <td>${formatNum(d.target_nomen)}</td>
                    <td class="text-end">${formatRp(d.target_nominal)}</td>
                    <td>${formatNum(d.realisasi_nomen)}</td>
                    <td class="text-end text-success">${formatRp(d.realisasi_nominal)}</td>
                    <td><span class="badge ${plbr >= 100 ? 'bg-success' : 'bg-warning text-dark'}">${plbr}%</span></td>
                    <td><span class="badge ${prp >= 100 ? 'bg-success' : 'bg-primary'}">${prp}%</span></td>
                </tr>
            `);
        });
    });
}

/**
 * Inisialisasi dan Update Grafik Chart.js
 */
function loadCharts() {
    // 1. Grafik Batang Rayon
    $.get('/api/breakdown_rayon', function(d) {
        const ctx = document.getElementById('chartRayon');
        if (!ctx) return;
        
        if (chartRayon) chartRayon.destroy();
        chartRayon = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: d.map(x => 'Rayon ' + x.rayon),
                datasets: [{
                    label: 'Realisasi (Rp)',
                    data: d.map(x => x.total_collection),
                    backgroundColor: ['#1e3c72', '#198754'],
                    borderRadius: 5
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } }
            }
        });
    });

    // 2. Grafik Tren Kumulatif
    $.get('/api/tren_harian', function(d) {
        const ctx = document.getElementById('chartTren');
        if (!ctx) return;
        
        if (chartTren) chartTren.destroy();
        chartTren = new Chart(ctx, {
            type: 'line',
            data: {
                labels: d.map(x => x.tgl_bayar.split('-')[2]), // Ambil tanggal (DD)
                datasets: [{
                    label: 'Kumulatif (Rp)',
                    data: d.map(x => x.kumulatif),
                    borderColor: '#0d6efd',
                    backgroundColor: 'rgba(13, 110, 253, 0.1)',
                    fill: true,
                    tension: 0.3
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                scales: {
                    y: { 
                        beginAtZero: true,
                        ticks: { callback: (val) => (val / 1000000).toFixed(0) + ' Jt' }
                    }
                }
            }
        });
    });
}

/**
 * Inisialisasi DataTables untuk Detail Collection
 */
function loadCollectionData(area = 'SUNTER') {
    if ($.fn.DataTable.isDataTable('#tableColl')) {
        // Jika sudah ada, update URL dan reload
        tableColl.ajax.url(`/api/collection_data?rayon=${area}`).load();
        return;
    }

    tableColl = $('#tableColl').DataTable({
        ajax: {
            url: `/api/collection_data?rayon=${area}`,
            dataSrc: ''
        },
        columns: [
            { data: 'tgl_bayar' },
            { 
                data: 'rayon',
                render: (data) => `<span class="badge bg-${data == '34' ? 'primary' : 'success'}">Rayon ${data}</span>`
            },
            { data: 'nomen' },
            { data: 'nama', defaultContent: '<span class="text-muted italic">Tanpa Nama</span>' },
            { 
                data: 'jumlah_bayar', 
                className: 'text-end fw-bold text-success',
                render: (v) => formatRp(v)
            }
        ],
        order: [[0, 'desc']],
        pageLength: 10,
        language: {
            url: "//cdn.datatables.net/plug-ins/1.13.6/i18n/id.json"
        }
    });
}

/**
 * Fungsi Pencarian Global dari Header
 */
function cariPelanggan() {
    const val = $('#globalSearch').val();
    if (!val) {
        alert("Masukkan Nomen atau Nama pelanggan.");
        return;
    }

    // Paksa pindah ke tab Collection
    const triggerEl = document.querySelector('#collection-tab');
    const tab = new bootstrap.Tab(triggerEl);
    tab.show();

    // Tunggu render tab selesai lalu search
    setTimeout(() => {
        if (tableColl) {
            tableColl.search(val).draw();
        } else {
            loadCollectionData();
            setTimeout(() => tableColl.search(val).draw(), 500);
        }
    }, 200);
}

// --- INITIALIZATION ---

$(document).ready(function() {
    console.log("🚀 Dashboard JS Loaded.");

    // 1. Muat data awal (Ringkasan)
    updateKPI();
    loadCharts();

    // 2. Handle perubahan filter Area
    $('#filterArea').change(function() {
        const area = $(this).val();
        // Jika tab Collection sedang aktif, reload tabel
        if ($('#tab-collection').hasClass('active')) {
            loadCollectionData(area);
        }
        // Always refresh charts based on global context if API supports it
        loadCharts();
    });

    // 3. Handle Tab Switch (Lazy Load Tabel)
    $('button[data-bs-toggle="tab"]').on('shown.bs.tab', function (e) {
        const targetId = $(e.target).data('bs-target');
        if (targetId === '#tab-collection') {
            loadSummaryTable();
            loadCollectionData($('#filterArea').val());
        } else if (targetId === '#tab-ringkasan') {
            updateKPI();
            loadCharts();
        }
    });

    // 4. Handle Radio Buttons di Modal Upload (Visual Feedback)
    $('.upload-option').click(function() {
        $('.upload-option input[type="radio"]').prop('checked', false);
        $(this).find('input[type="radio"]').prop('checked', true);
    });

    // 5. Inisialisasi Tooltips Bootstrap
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });
});

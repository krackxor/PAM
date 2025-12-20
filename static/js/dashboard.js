/**
 * DASHBOARD LOGIC
 * File ini menangani semua interaksi, grafik, dan update data.
 */

let chartRayonInstance = null;
let chartTrenInstance = null;
let tableCollectionInstance = null;

// --- FORMATTER ---
const formatRupiah = (angka) => {
    return 'Rp ' + new Intl.NumberFormat('id-ID').format(angka);
};

// --- CORE FUNCTIONS ---

// 1. Update KPI Cards (Header Ringkasan)
function updateKPI() {
    $.get('/api/kpi_data', function(data) {
        console.log("KPI Data:", data);

        // Update Text Elements
        $('#kpi-total-pelanggan').text(new Intl.NumberFormat('id-ID').format(data.total_pelanggan));
        $('#labelPeriode').text(data.periode);

        // Target
        $('#kpi-target-total').text(formatRupiah(data.target.total_nominal));
        $('#kpi-target-nomen-total').text(data.target.total_nomen + ' nomen');
        $('#kpi-target-bayar').text(formatRupiah(data.target.sudah_bayar_nominal));
        $('#kpi-target-nomen-bayar').text(data.target.sudah_bayar_nomen + ' nomen');
        $('#kpi-target-belum').text(formatRupiah(data.target.belum_bayar_nominal));
        $('#kpi-target-nomen-belum').text(data.target.belum_bayar_nomen + ' nomen');

        // Collection
        $('#kpi-coll-total').text(formatRupiah(data.collection.total_nominal));
        $('#kpi-coll-nomen-total').text(data.collection.total_nomen + ' nomen');
        $('#kpi-coll-current').text(formatRupiah(data.collection.current_nominal));
        $('#kpi-coll-nomen-current').text(data.collection.current_nomen + ' nomen');
        $('#kpi-coll-undue').text(formatRupiah(data.collection.undue_nominal));
        $('#kpi-coll-nomen-undue').text(data.collection.undue_nomen + ' nomen');

        // Rate
        $('#kpi-rate').text(data.collection_rate + '%');
        $('#kpi-rate-bar').css('width', data.collection_rate + '%');

        // Tunggakan
        $('#kpi-tunggakan-total').text(formatRupiah(data.tunggakan.total_nominal));
        $('#kpi-tunggakan-nomen-total').text(data.tunggakan.total_nomen + ' nomen');
        $('#kpi-tunggakan-bayar').text(formatRupiah(data.tunggakan.sudah_bayar_nominal));
        $('#kpi-tunggakan-nomen-bayar').text(data.tunggakan.sudah_bayar_nomen + ' nomen');
        $('#kpi-tunggakan-belum').text(formatRupiah(data.tunggakan.belum_bayar_nominal));
        $('#kpi-tunggakan-nomen-belum').text(data.tunggakan.belum_bayar_nomen + ' nomen');

    }).fail(function(err) {
        console.error("Gagal ambil KPI:", err);
    });
}

// 2. Grafik Komposisi Rayon
function loadChartRayon() {
    $.get('/api/breakdown_rayon', function(data) {
        const ctx = document.getElementById('chartRayon');
        if (!ctx) return;

        if (chartRayonInstance) chartRayonInstance.destroy();

        chartRayonInstance = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: data.map(d => 'Rayon ' + d.rayon),
                datasets: [{
                    label: 'Collection (Rp)',
                    data: data.map(d => d.total_collection),
                    backgroundColor: ['#0d6efd', '#198754'],
                    borderWidth: 0,
                    borderRadius: 4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                return formatRupiah(context.raw);
                            }
                        }
                    }
                },
                scales: {
                    y: { 
                        beginAtZero: true,
                        ticks: {
                            callback: function(value) {
                                return (value / 1000000).toFixed(0) + ' Jt';
                            }
                        }
                    }
                }
            }
        });
    });
}

// 3. Grafik Tren Harian
function loadChartTren() {
    $.get('/api/tren_harian', function(data) {
        const ctx = document.getElementById('chartTren');
        if (!ctx) return;

        if (chartTrenInstance) chartTrenInstance.destroy();

        chartTrenInstance = new Chart(ctx, {
            type: 'line',
            data: {
                labels: data.map(d => {
                    const date = new Date(d.tgl_bayar);
                    return date.getDate(); // Ambil tanggalnya saja
                }),
                datasets: [{
                    label: 'Harian',
                    data: data.map(d => d.total_harian),
                    borderColor: '#198754', // Hijau
                    backgroundColor: 'rgba(25, 135, 84, 0.1)',
                    fill: true,
                    tension: 0.3
                }, {
                    label: 'Kumulatif',
                    data: data.map(d => d.kumulatif),
                    borderColor: '#0d6efd', // Biru
                    backgroundColor: 'rgba(13, 110, 253, 0.05)',
                    fill: true,
                    tension: 0.3,
                    yAxisID: 'y1'
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: {
                    mode: 'index',
                    intersect: false,
                },
                scales: {
                    y: {
                        type: 'linear',
                        display: true,
                        position: 'left',
                        ticks: {
                            callback: function(value) { return (value / 1000000).toFixed(0) + ' Jt'; }
                        }
                    },
                    y1: {
                        type: 'linear',
                        display: true,
                        position: 'right',
                        grid: { drawOnChartArea: false },
                        ticks: {
                            callback: function(value) { return (value / 1000000000).toFixed(1) + ' M'; }
                        }
                    }
                }
            }
        });
    });
}

// 4. Tabel Detail Collection
function loadCollectionData() {
    // Jika tabel sudah ada, reload saja datanya (jangan di-init ulang)
    if ($.fn.DataTable.isDataTable('#tableColl')) {
        $('#tableColl').DataTable().ajax.reload();
        return;
    }

    tableCollectionInstance = $('#tableColl').DataTable({
        ajax: {
            url: '/api/collection_data',
            dataSrc: ''
        },
        pageLength: 10,
        responsive: true,
        order: [[0, 'desc']], // Urutkan tanggal terbaru
        language: {
            url: "//cdn.datatables.net/plug-ins/1.13.6/i18n/id.json"
        },
        columns: [
            { data: 'tgl_bayar' },
            { 
                data: 'rayon',
                render: function(data) {
                    let color = data === '34' ? 'primary' : 'success';
                    return `<span class="badge bg-${color}">Rayon ${data}</span>`;
                }
            },
            { data: 'nomen' },
            { 
                data: 'nama',
                defaultContent: '<span class="text-muted fst-italic">Tanpa Nama</span>'
            },
            { 
                data: 'jumlah_bayar',
                className: 'text-end fw-bold text-success',
                render: $.fn.dataTable.render.number('.', ',', 0, 'Rp ')
            }
        ]
    });
}

// 5. Fitur Pencarian Global
function cariPelanggan() {
    let val = $('#globalSearch').val();
    if(val) {
        // Pindah ke tab collection
        const triggerEl = document.querySelector('button[data-bs-target="#tab-collection"]');
        const tab = new bootstrap.Tab(triggerEl);
        tab.show();

        // Load data jika belum ada, lalu filter
        if (tableCollectionInstance) {
            tableCollectionInstance.search(val).draw();
        } else {
            loadCollectionData();
            setTimeout(() => {
                tableCollectionInstance.search(val).draw();
            }, 500);
        }
    } else {
        alert("Silakan masukkan Nomen atau Nama pelanggan.");
    }
}

// --- DOCUMENT READY ---
$(document).ready(function() {
    console.log("🚀 Dashboard Initialized");

    // Load data pertama kali
    updateKPI();
    loadChartRayon();
    loadChartTren();

    // Event saat tab Collection dibuka
    $('button[data-bs-target="#tab-collection"]').on('shown.bs.tab', function (e) {
        loadCollectionData(); // Load tabel hanya saat diperlukan
    });

    // Inisialisasi Tooltip Bootstrap
    var tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'))
    var tooltipList = tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl)
    });

    // Logika UI Upload (Klik Card Radio)
    $('.upload-option').click(function() {
        // Reset
        $('.upload-option input[type="radio"]').prop('checked', false);
        
        // Set Selected
        $(this).find('input[type="radio"]').prop('checked', true);
        console.log("Selected Type:", $(this).find('input[type="radio"]').val());
    });

    // Auto Refresh setiap 5 menit
    setInterval(updateKPI, 300000);
});

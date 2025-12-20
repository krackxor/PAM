// Init Icons
lucide.createIcons();

// Variabel Global
let currentArea = 'SUNTER';
let trendChartInstance = null;
let pieChartInstance = null;
let dataTableInstance = null;

document.addEventListener('DOMContentLoaded', function () {
    console.log("🚀 Dashboard Ready. Initializing...");

    // 1. Inisialisasi Chart Kosong (Placeholder)
    initCharts();
    
    // 2. Load Data Awal
    updateDashboardData(currentArea);

    // 3. Listener Filter Area
    const filterSelect = document.getElementById('filterArea');
    if(filterSelect) {
        filterSelect.addEventListener('change', function() {
            currentArea = this.value;
            console.log("🔄 Filter Changed to: " + currentArea);
            updateDashboardData(currentArea);
        });
    }
});

// --- FUNGSI UPDATE DATA UTAMA ---
function updateDashboardData(area) {
    // A. Update KPI Cards
    fetch('/api/kpi_data')
        .then(res => res.json())
        .then(data => {
            // Helper formatter
            const fmt = (val) => new Intl.NumberFormat('id-ID').format(val);
            const fmtRp = (val) => "Rp " + new Intl.NumberFormat('id-ID', { maximumFractionDigits: 0 }).format(val);

            // Update DOM Elements Safely
            const elTotal = document.getElementById('kpi-total');
            if(elTotal) elTotal.innerText = fmt(data.total_pelanggan);

            const elTarget = document.getElementById('kpi-target');
            if(elTarget) elTarget.innerText = fmtRp(data.target.total_nominal);

            const elRealisasi = document.getElementById('kpi-realisasi');
            if(elRealisasi) elRealisasi.innerText = fmtRp(data.collection.total_nominal);

            const elRate = document.getElementById('kpi-rate');
            if(elRate) elRate.innerText = data.collection_rate + "%";

            const elBar = document.getElementById('kpi-bar');
            if(elBar) elBar.style.width = data.collection_rate + "%";

            const elTunggakan = document.getElementById('kpi-tunggakan');
            if(elTunggakan) elTunggakan.innerText = fmtRp(data.tunggakan.total_nominal);
            
            const elPeriode = document.getElementById('periodeDisplay');
            if(elPeriode) elPeriode.innerText = "Periode: " + data.periode;
        })
        .catch(err => console.error("Error fetching KPI:", err));

    // B. Update Chart Tren Harian
    fetch('/api/tren_harian')
        .then(res => res.json())
        .then(data => {
            const labels = data.map(d => d.tgl_bayar);
            const values = data.map(d => d.kumulatif); // Pakai kumulatif agar grafik naik
            
            if(trendChartInstance) {
                trendChartInstance.data.labels = labels;
                trendChartInstance.data.datasets[0].data = values;
                trendChartInstance.update();
            }
        });

    // C. Update Chart Pie (Komposisi)
    fetch('/api/breakdown_rayon')
        .then(res => res.json())
        .then(data => {
            // Asumsi data: [{rayon: '34', ...}, {rayon: '35', ...}]
            const values = [];
            // Mapping sederhana untuk pie chart
            const r34 = data.find(d => d.rayon == '34');
            const r35 = data.find(d => d.rayon == '35');
            values.push(r34 ? r34.total_target : 0);
            values.push(r35 ? r35.total_target : 0);

            if(pieChartInstance) {
                pieChartInstance.data.datasets[0].data = values;
                pieChartInstance.update();
            }
        });

    // D. Update DataTables
    initDataTable(area);
}

// --- INISIALISASI CHART ---
function initCharts() {
    // 1. Line Chart
    const ctxTren = document.getElementById('chartTren');
    if (ctxTren) {
        trendChartInstance = new Chart(ctxTren.getContext('2d'), {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: 'Kumulatif Collection (Rp)',
                    data: [],
                    borderColor: '#2563eb',
                    backgroundColor: 'rgba(37, 99, 235, 0.1)',
                    borderWidth: 2,
                    fill: true,
                    tension: 0.4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: {
                    y: { 
                        beginAtZero: true,
                        grid: { borderDash: [2, 4] },
                        ticks: { callback: function(val) { return (val/1000000).toFixed(0) + ' Jt'; } }
                    },
                    x: { grid: { display: false } }
                }
            }
        });
    }

    // 2. Pie Chart
    const ctxPie = document.getElementById('chartPie');
    if (ctxPie) {
        pieChartInstance = new Chart(ctxPie.getContext('2d'), {
            type: 'doughnut',
            data: {
                labels: ['Rayon 34', 'Rayon 35'],
                datasets: [{
                    data: [50, 50],
                    backgroundColor: ['#2563eb', '#0ea5e9'],
                    borderWidth: 0
                }]
            },
            options: {
                responsive: true,
                cutout: '70%',
                plugins: { legend: { display: false } }
            }
        });
    }
}

// --- INISIALISASI DATATABLES ---
function initDataTable(area) {
    // Hancurkan instance lama jika ada (untuk refresh filter)
    if (dataTableInstance) {
        dataTableInstance.destroy();
    }

    // Cek apakah tabel ada di halaman sebelum inisialisasi
    if (!$('#tableCollection').length) return;

    // AJAX URL dengan parameter rayon
    const apiUrl = `/api/collection_data?rayon=${area}`;

    dataTableInstance = $('#tableCollection').DataTable({
        ajax: {
            url: apiUrl,
            dataSrc: '' // API mengembalikan array of objects langsung
        },
        columns: [
            { data: 'tgl_bayar' },
            { data: 'nomen' },
            { data: 'nama' },
            { data: 'rayon' },
            { 
                data: 'target_mc',
                render: $.fn.dataTable.render.number('.', ',', 0, 'Rp ')
            },
            { 
                data: 'jumlah_bayar',
                render: $.fn.dataTable.render.number('.', ',', 0, 'Rp ') 
            }
        ],
        order: [[0, 'desc']], // Urutkan tanggal descending
        pageLength: 10,
        language: {
            url: '//cdn.datatables.net/plug-ins/1.13.6/i18n/id.json'
        }
    });
}

// --- UI HELPERS ---
function toggleModal(id) {
    const modal = document.getElementById(id);
    if(modal) {
        modal.classList.toggle('hidden');
    }
}

function switchTab(tabId) {
    document.querySelectorAll('.tab-content').forEach(el => el.classList.add('hidden'));
    const target = document.getElementById(tabId);
    if (target) target.classList.remove('hidden');
    
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.classList.remove('active', 'text-blue-600', 'border-blue-600');
        btn.classList.add('text-slate-500');
    });
    const activeBtn = document.querySelector(`.tab-btn[data-target="${tabId}"]`);
    if(activeBtn) {
        activeBtn.classList.add('active', 'text-blue-600', 'border-blue-600');
        activeBtn.classList.remove('text-slate-500');
    }
}

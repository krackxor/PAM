/**
 * SUNTER DASHBOARD MAIN LOGIC
 * Mengatur interaksi Filter Global, Chart, dan Tabel Data.
 */

document.addEventListener('DOMContentLoaded', function () {
    
    // ==================================================
    // 1. INISIALISASI VARIABEL GLOBAL
    // ==================================================
    let currentArea = 'SUNTER'; // Default Filter
    let collectionTable; // Instance DataTable
    let trendChart; // Instance Chart.js

    console.log("🚀 Dashboard JS Loaded. Area saat ini:", currentArea);

    // ==================================================
    // 2. LOGIKA GLOBAL FILTER (HEADER)
    // ==================================================
    const filterSelect = document.getElementById('filterArea');
    
    if (filterSelect) {
        filterSelect.addEventListener('change', function() {
            currentArea = this.value;
            console.log("🔄 Filter Berubah: " + currentArea);
            
            // Tampilkan Loading Indicator (Opsional/Visual Feedback)
            showLoadingState();

            // Panggil fungsi update data (Simulasi AJAX)
            updateDashboardData(currentArea);
        });
    }

    // ==================================================
    // 3. INISIALISASI CHART (TAB RINGKASAN)
    // ==================================================
    const ctx = document.getElementById('trendChart');
    if (ctx) {
        trendChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: ['1 Mei', '2 Mei', '3 Mei', '4 Mei', '5 Mei', '6 Mei', '7 Mei'],
                datasets: [{
                    label: 'Collection Harian (Juta Rp)',
                    data: [120, 150, 180, 140, 200, 220, 250], // Data Dummy Awal
                    borderColor: '#0d6efd',
                    backgroundColor: 'rgba(13, 110, 253, 0.1)',
                    borderWidth: 2,
                    fill: true,
                    tension: 0.4 // Garis melengkung halus
                }]
            },
            options: {
                responsive: true,
                plugins: {
                    legend: { display: false } // Sembunyikan legend agar bersih
                },
                scales: {
                    y: { beginAtZero: true }
                }
            }
        });
    }

    // ==================================================
    // 4. INISIALISASI DATATABLES (TAB COLLECTION)
    // ==================================================
    // Kita cek apakah tabel ada di DOM sebelum init
    if ($('#tableCollection').length) {
        collectionTable = $('#tableCollection').DataTable({
            responsive: true,
            language: {
                search: "Cari Data:",
                lengthMenu: "Tampilkan _MENU_ baris",
                info: "Menampilkan _START_ s/d _END_ dari _TOTAL_ data",
                paginate: {
                    first: "Awal",
                    last: "Akhir",
                    next: "➡️",
                    previous: "⬅️"
                }
            },
            // Simulasi Kolom Excel
            columns: [
                { title: "Tanggal" },
                { title: "Rayon" },
                { title: "Jml Cust" },
                { title: "Target MC (Rp)" },
                { title: "Realisasi (Rp)" },
                { title: "% Capaian" },
                { title: "Status" }
            ]
        });

        // Isi Data Dummy Awal ke Tabel
        const dummyData = [
            ["2024-05-25", "34", "150", "50.000.000", "45.000.000", "90%", "<span class='badge bg-success'>OK</span>"],
            ["2024-05-25", "35", "120", "40.000.000", "20.000.000", "50%", "<span class='badge bg-warning'>Low</span>"],
        ];
        
        dummyData.forEach(row => {
            collectionTable.row.add(row).draw();
        });
    }

    // ==================================================
    // 5. FUNGSI UPDATE DATA (SIMULASI AJAX)
    // ==================================================
    function updateDashboardData(area) {
        // Di sini nanti kita pakai fetch() ke Python Flask
        // Contoh logika sederhana untuk demo:
        
        let multiplier = 1;
        if (area === '34') multiplier = 0.6;
        if (area === '35') multiplier = 0.4;

        // 1. Update Angka KPI (Simulasi DOM Manipulation)
        const kpiElement = document.querySelector('.card-kpi h2.text-success');
        if(kpiElement) {
            let baseVal = 125000000; // Contoh nilai dasar
            let newVal = baseVal * multiplier;
            kpiElement.innerText = "Rp " + new Intl.NumberFormat('id-ID').format(newVal);
        }

        // 2. Update Chart
        if (trendChart) {
            // Ubah data chart acak agar terlihat 'hidup'
            const newData = [120, 150, 180, 140, 200, 220, 250].map(val => val * multiplier);
            trendChart.data.datasets[0].data = newData;
            trendChart.update();
        }

        // 3. Update Tabel Collection
        if (collectionTable) {
            collectionTable.clear();
            if (area === 'SUNTER' || area === '34') {
                collectionTable.row.add(["2024-05-25", "34", "150", "50.000.000", "45.000.000", "90%", "<span class='badge bg-success'>OK</span>"]);
            }
            if (area === 'SUNTER' || area === '35') {
                collectionTable.row.add(["2024-05-25", "35", "120", "40.000.000", "20.000.000", "50%", "<span class='badge bg-warning'>Low</span>"]);
            }
            collectionTable.draw();
        }

        // Selesai Loading
        setTimeout(() => {
             // Hilangkan loading state jika ada
             console.log("✅ Data updated for: " + area);
        }, 500);
    }

    function showLoadingState() {
        // Bisa tambahkan spinner overlay di sini nanti
    }

});

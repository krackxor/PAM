{% extends "base.html" %}

{% block title %}Laporan Piutang & Koleksi{% endblock %}

{% block custom_styles %}
.summary-container { padding: 20px; }
.kpi-card {
    background-color: #fff; padding: 20px; border-radius: 8px;
    box-shadow: 0 0.15rem 1.75rem 0 rgba(58, 59, 69, 0.15);
    margin-bottom: 20px; text-align: center; border: 1px solid #e3e6f0;
}
.kpi-card h4 { color: #5a5c69; font-size: 0.8rem; font-weight: 700; text-transform: uppercase; margin-bottom: 10px; }
.kpi-card p { font-size: 1.5rem; font-weight: 700; color: #4e73df; margin: 0; }
.grid-container { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 20px; margin-bottom: 30px; }

.section-card {
    background-color: #fff; padding: 20px; border-radius: 8px;
    box-shadow: 0 0.15rem 1.75rem 0 rgba(58, 59, 69, 0.15);
    margin-bottom: 30px; border: 1px solid #e3e6f0;
    display: flex; flex-direction: column; height: 100%;
}
.section-title { color: #4e73df; font-weight: 800; border-bottom: 2px solid #eee; padding-bottom: 10px; margin-bottom: 20px; }

.chart-container { height: 200px; position: relative; margin-bottom: 15px; }
.scrollable-table { max-height: 150px; overflow-y: auto; border: 1px solid #eaecf4; border-radius: 5px; margin-bottom: 15px; }
.table-details { font-size: 0.75rem; width: 100%; margin-bottom: 0; }
.table-details th { background-color: #f8f9fc; color: #4e73df; text-transform: uppercase; font-size: 0.65rem; position: sticky; top: 0; z-index: 2; }

.insight-box {
    background-color: #f8f9fc;
    border-left: 4px solid #4e73df;
    padding: 12px;
    border-radius: 4px;
    font-size: 0.72rem;
    color: #5a5c69;
    margin-top: auto;
}
.insight-box strong { color: #4e73df; display: block; margin-bottom: 4px; text-transform: uppercase; font-size: 0.65rem; }

.btn-group-filter .btn { font-size: 0.65rem; font-weight: 700; padding: 2px 8px; }
.btn-group-filter .btn.active { background-color: #4e73df; color: white; border-color: #4e73df; }

.nav-pills .nav-link { font-weight: bold; color: #5a5c69; font-size: 0.9rem; }
.nav-pills .nav-link.active { background-color: #4e73df; }
{% endblock %}

{% block content %}
<div class="summary-container">
    <!-- Header: Filter & Download -->
    <div class="d-flex justify-content-between align-items-center mb-4 flex-wrap">
        <h2 class="section-title border-0 mb-0"><i class="fas fa-chart-line mr-2"></i> Laporan Piutang & Koleksi</h2>
        <div class="d-flex align-items-center">
            <a href="{{ url_for('bp_collection.download_summary_csv', period=period) }}" class="btn btn-sm btn-success shadow-sm mr-3">
                <i class="fas fa-file-excel mr-1"></i> Download All (CSV)
            </a>
            <form class="form-inline" method="get">
                <input type="month" class="form-control form-control-sm mr-2 shadow-sm" name="period" value="{{ period }}">
                <button type="submit" class="btn btn-sm btn-primary shadow-sm px-3">Filter</button>
            </form>
        </div>
    </div>

    <!-- Tab Navigasi -->
    <ul class="nav nav-pills mb-4 bg-white p-2 rounded shadow-sm border" id="pills-tab" role="tablist">
        <li class="nav-item"><a class="nav-link active" data-toggle="pill" href="#pills-piutang">Piutang (MC)</a></li>
        <li class="nav-item"><a class="nav-link" data-toggle="pill" href="#pills-tunggakan">Tunggakan (AR)</a></li>
        <li class="nav-item"><a class="nav-link" data-toggle="pill" href="#pills-collection">Koleksi (MB)</a></li>
    </ul>

    <div class="tab-content">
        {% for cat in ['piutang', 'tunggakan', 'collection'] %}
        <div class="tab-pane fade {% if cat == 'piutang' %}show active{% endif %}" id="pills-{{ cat }}">
            
            <!-- KPI Cards Row -->
            <div class="grid-container">
                <div class="kpi-card" style="border-left: 4px solid #4e73df;">
                    <h4>Jumlah Nomen</h4>
                    <p id="count-{{ cat }}"><i class="fas fa-spinner fa-spin text-muted"></i></p>
                </div>
                <div class="kpi-card" style="border-left: 4px solid #1cc88a;">
                    <h4>Total Pemakaian</h4>
                    <p id="usage-{{ cat }}"><i class="fas fa-spinner fa-spin text-muted"></i></p>
                </div>
                <div class="kpi-card" style="border-left: 4px solid #f6c23e;">
                    <h4>Total Nominal</h4>
                    <p id="nominal-{{ cat }}"><i class="fas fa-spinner fa-spin text-muted"></i></p>
                </div>
            </div>

            <!-- Kontainer Grid Kontributor -->
            <div id="container-data-{{ cat }}">
                <div class="row">
                    {% for type, label in [('rayon', 'Rayon'), ('pcez', 'PCEZ'), ('pc', 'Petugas (PC)'), ('ez', 'EZ'), ('block', 'Block'), ('tarif', 'Tarif')] %}
                    <div class="col-lg-6 col-xl-4 mb-4">
                        <div class="section-card">
                            <div class="d-flex justify-content-between align-items-center mb-3">
                                <h6 class="m-0 font-weight-bold text-primary">{{ label }}</h6>
                                <div class="btn-group btn-group-toggle btn-group-filter">
                                    <button class="btn btn-outline-primary active" onclick="updateFilter('{{ cat }}', '{{ type }}', 'ALL', this)">ALL</button>
                                    <button class="btn btn-outline-primary" onclick="updateFilter('{{ cat }}', '{{ type }}', '34', this)">34</button>
                                    <button class="btn btn-outline-primary" onclick="updateFilter('{{ cat }}', '{{ type }}', '35', this)">35</button>
                                </div>
                            </div>
                            
                            <!-- Grafik -->
                            <div class="chart-container">
                                <canvas id="chart-{{ type }}-{{ cat }}"></canvas>
                            </div>

                            <!-- Tabel Seluruh Data -->
                            <div class="scrollable-table">
                                <table class="table table-sm table-hover table-details" id="table-{{ type }}-{{ cat }}">
                                    <thead><tr><th>{{ label }}</th><th class="text-right">Rp</th></tr></thead>
                                    <tbody><tr><td colspan="2" class="text-center py-4"><i class="fas fa-spinner fa-spin"></i></td></tr></tbody>
                                </table>
                            </div>

                            <!-- Smart Insight -->
                            <div class="insight-box" id="insight-{{ type }}-{{ cat }}">
                                <strong><i class="fas fa-lightbulb mr-1"></i> Analisa & Saran</strong>
                                <span class="insight-text">Menunggu data...</span>
                            </div>
                        </div>
                    </div>
                    {% endfor %}
                </div>
            </div>

            <div id="empty-{{ cat }}" style="display:none;" class="alert alert-warning text-center p-5">
                <h4><i class="fas fa-exclamation-triangle"></i> Data Tidak Ditemukan</h4>
                <p>Tidak ada record untuk periode {{ period }}</p>
            </div>
        </div>
        {% endfor %}
    </div>
</div>

<script src="https://cdn.jsdelivr.net/npm/chart.js@3.7.0/dist/chart.min.js"></script>

<script>
    const currentPeriod = "{{ period }}";
    const rawStore = {}; 
    const chartInstances = {};

    function formatIDR(val) {
        return new Intl.NumberFormat('id-ID').format(val || 0);
    }

    async function loadKPI() {
        try {
            const res = await fetch(`/collection/api/stats_summary?period=${currentPeriod}`);
            const data = await res.json();
            ['piutang', 'tunggakan', 'collection'].forEach(c => {
                const s = data[c];
                if (s && s.totals && s.totals.count > 0) {
                    document.getElementById(`count-${c}`).innerText = formatIDR(s.totals.count);
                    document.getElementById(`usage-${c}`).innerText = formatIDR(s.totals.total_usage) + ' m³';
                    document.getElementById(`nominal-${c}`).innerText = 'Rp ' + formatIDR(s.totals.total_nominal);
                } else {
                    document.getElementById(`container-data-${c}`).style.display = 'none';
                    document.getElementById(`empty-${c}`).style.display = 'block';
                }
            });
        } catch (e) { console.error(e); }
    }

    async function loadData(cat, type) {
        try {
            const res = await fetch(`/collection/api/distribution/${type}?period=${currentPeriod}`);
            const json = await res.json();
            
            if(!rawStore[cat]) rawStore[cat] = {};
            rawStore[cat][type] = json.data;
            rawStore[cat][`${type}_field`] = json.category;

            renderUI(cat, type, 'ALL');
        } catch (e) { console.error(e); }
    }

    function renderUI(cat, type, filter) {
        const fullData = rawStore[cat][type];
        const field = rawStore[cat][`${type}_field`];
        
        let filtered = fullData;
        if (filter !== 'ALL') {
            filtered = fullData.filter(item => String(item[field] || '').startsWith(filter));
        }

        // 1. Update Tabel
        const tableBody = document.querySelector(`#table-${type}-${cat} tbody`);
        if(filtered.length === 0) {
            tableBody.innerHTML = '<tr><td colspan="2" class="text-center text-muted">Tidak ada data</td></tr>';
        } else {
            tableBody.innerHTML = filtered.map(item => `
                <tr>
                    <td><strong>${item[field] || 'N/A'}</strong></td>
                    <td class="text-right font-weight-bold text-primary">${formatIDR(item.total_piutang)}</td>
                </tr>
            `).join('');
        }

        // 2. Update Grafik (Top 10 saja agar tidak sumpek)
        const ctx = document.getElementById(`chart-${type}-${cat}`).getContext('2d');
        const chartId = `${type}-${cat}`;
        if (chartInstances[chartId]) chartInstances[chartId].destroy();

        const chartData = filtered.slice(0, 10);
        chartInstances[chartId] = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: chartData.map(d => d[field] || 'N/A'),
                datasets: [{
                    label: 'Nominal Rp',
                    data: chartData.map(d => d.total_piutang),
                    backgroundColor: filter === '34' ? '#1cc88a' : (filter === '35' ? '#f6c23e' : '#4e73df'),
                    borderRadius: 4
                }]
            },
            options: {
                responsive: true, maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: {
                    y: { beginAtZero: true, ticks: { font: { size: 8 }, callback: v => v >= 1000000 ? (v/1000000).toFixed(1) + 'jt' : formatIDR(v) } },
                    x: { ticks: { font: { size: 8 } } }
                }
            }
        });

        // 3. Update Smart Insight
        generateInsight(cat, type, filter, filtered);
    }

    function generateInsight(cat, type, filter, data) {
        const box = document.querySelector(`#insight-${type}-${cat} .insight-text`);
        if(!data || data.length === 0) {
            box.innerText = "Belum ada data untuk dianalisa.";
            return;
        }

        const topItem = data[0];
        const topName = topItem[rawStore[cat][`${type}_field`]];
        const context = cat === 'piutang' ? 'Potensi Pendapatan' : (cat === 'tunggakan' ? 'Beban Piutang' : 'Pencapaian Koleksi');

        let analisa = `Ditemukan kontribusi ${context} tertinggi pada ${type.toUpperCase()} <b>${topName}</b>.`;
        let saran = "";

        if (cat === 'piutang' || cat === 'tunggakan') {
            saran = `Disarankan untuk memperketat monitoring penagihan di zona ini melalui tim lapangan.`;
            if (type === 'pc') saran = `Evaluasi beban kerja Petugas PC ${topName} karena memiliki angka piutang paling mencolok.`;
            if (type === 'block') saran = `Lakukan audit fisik meter di area Block ${topName} untuk memastikan akurasi pembacaan.`;
        } else {
            saran = `Pertahankan performa di zona ini dan jadikan benchmark untuk zona dengan tingkat koleksi rendah.`;
        }

        const masaDepan = `<b>Target ke Depan:</b> Lakukan pemetaan ulang cluster di wilayah ${filter !== 'ALL' ? filter : '34 & 35'} guna menekan angka penunggakan di atas 2 bulan.`;

        box.innerHTML = `${analisa} ${saran}<br><br>${masaDepan}`;
    }

    function updateFilter(cat, type, mode, btn) {
        const parent = btn.parentElement;
        parent.querySelectorAll('button').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        renderUI(cat, type, mode);
    }

    document.addEventListener('DOMContentLoaded', () => {
        loadKPI();
        const types = ['rayon', 'pcez', 'pc', 'ez', 'block', 'tarif'];
        ['piutang', 'tunggakan', 'collection'].forEach(c => {
            types.forEach(t => loadData(c, t));
        });
    });
</script>
{% endblock %}

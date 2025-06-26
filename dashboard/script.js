document.addEventListener("DOMContentLoaded", () => {
  // --- Global variables ---
  const API_BASE_URL = "http://localhost:8000";
  let hitRateChart = null;

  // --- UI Elements ---
  const kpiElements = {
    totalProducts: document.getElementById("kpi-total-products"),
    avgScore: document.getElementById("kpi-avg-score"),
    totalHits: document.getElementById("kpi-total-hits"),
    lastUpdated: document.getElementById("last-updated"),
  };
  const productTableBody = document.getElementById("product-table-body");
  const predictionOutput = document.getElementById("prediction-output");
  const modelVersionOutput = document.getElementById("model-version-output");
  const simulatorSliders = document.querySelectorAll('#prediction-form input[type="range"]');

  // --- Functions ---
  const loadDashboardData = async () => {
    try {
      const response = await fetch(`${API_BASE_URL}/api/dashboard`);
      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || `HTTP error! status: ${response.status}`);
      }
      const data = await response.json();
      updateKPIs(data.kpis);
      populateProductTable(data.product_list);
    } catch (error) {
      console.error("Failed to load dashboard data:", error);
      productTableBody.innerHTML = `<tr><td colspan="5" style="text-align:center; color:#f43f5e;">Error: ${error.message}</td></tr>`;
    }
  };

  const updateKPIs = (kpis) => {
    kpiElements.totalProducts.textContent = kpis.total_products.toLocaleString();
    kpiElements.avgScore.textContent = kpis.avg_score;
    kpiElements.totalHits.textContent = kpis.total_hits.toLocaleString();
    kpiElements.lastUpdated.textContent = `Last Updated: ${kpis.last_updated}`;
    
    // Initialize or update chart with DARK THEME options
    const hitRate = kpis.hit_percentage;
    const missRate = 100 - hitRate;
    const chartCtx = document.getElementById('hit-rate-chart').getContext('2d');
    
    if(hitRateChart) {
      hitRateChart.destroy();
    }
    hitRateChart = new Chart(chartCtx, {
      type: 'doughnut',
      data: {
        labels: ['Hit', 'Not Hit'],
        datasets: [{
          data: [hitRate, missRate],
          backgroundColor: ['#22d3ee', '#374151'], // Accent Cyan & Border Color
          borderColor: '#1f2937', // Card Background
          borderWidth: 4
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        cutout: '70%',
        plugins: {
          legend: { display: false },
          tooltip: {
            backgroundColor: '#111827',
            titleColor: '#f9fafb',
            bodyColor: '#9ca3b0',
          }
        }
      }
    });
  };

  const populateProductTable = (products) => {
    productTableBody.innerHTML = ""; // Clear existing rows
    if (!products || products.length === 0) {
      productTableBody.innerHTML = `<tr><td colspan="5" style="text-align:center;">No product data available.</td></tr>`;
      return;
    }
    products.forEach(p => {
      const hitClass = p.is_hit === 1 ? 'color: #4ade80;' : 'color: #f43f5e;';
      const row = `
        <tr>
          <td>${p.ProductId}</td>
          <td>${p.avg_score.toFixed(2)}</td>
          <td>${p.review_count}</td>
          <td>${p.avg_helpfulness.toFixed(2)}</td>
          <td style="${hitClass}">${p.is_hit === 1 ? '✅ Hit' : '❌ Not Hit'}</td>
        </tr>
      `;
      productTableBody.innerHTML += row;
    });
  };

  const runPrediction = async () => {
    const features = {};
    simulatorSliders.forEach(slider => {
      features[slider.id] = parseFloat(slider.value);
    });

    try {
      const response = await fetch(`${API_BASE_URL}/predict/product-hit-predictor`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ features }),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || `HTTP error! status: ${response.status}`);
      }
      
      const data = await response.json();
      if (data.prediction === 1) {
        predictionOutput.textContent = "🎉 It's a HIT! 🎉";
        predictionOutput.className = 'hit';
      } else {
        predictionOutput.textContent = "😞 Not a hit.";
        predictionOutput.className = 'miss';
      }
      modelVersionOutput.textContent = `Model Version: ${data.model_version}`;
    } catch (error) {
      predictionOutput.textContent = `Error: ${error.message}`;
      predictionOutput.className = 'miss';
      modelVersionOutput.textContent = "Please check API service and inputs.";
    }
  };

  // --- Event Listeners ---
  simulatorSliders.forEach(slider => {
    const valueSpan = document.getElementById(`${slider.id}_val`);
    valueSpan.textContent = slider.value; // Set initial value
    
    slider.addEventListener('input', (event) => {
      valueSpan.textContent = event.target.value;
      runPrediction(); // Run prediction in real-time as slider moves
    });
  });

  // --- Initial Load ---
  loadDashboardData();
  runPrediction(); // Run initial prediction with default slider values
});
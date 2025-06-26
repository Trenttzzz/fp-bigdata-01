document
  .getElementById("prediction-form")
  .addEventListener("submit", async function (event) {
    event.preventDefault();

    const features = {
      avg_sentiment: parseFloat(document.getElementById("avg_sentiment").value),
      avg_score: parseFloat(document.getElementById("avg_score").value),
      review_count: parseInt(document.getElementById("review_count").value, 10),
      score_stddev: parseFloat(document.getElementById("score_stddev").value),
      avg_helpfulness: parseFloat(
        document.getElementById("avg_helpfulness").value
      ),
    };

    const requestBody = {
      features: features,
    };

    const resultContainer = document.getElementById("result-container");
    const predictionOutput = document.getElementById("prediction-output");
    const modelVersionOutput = document.getElementById("model-version-output");

    // API is on port 8000
    const apiUrl = "http://localhost:8000/predict/product-hit-predictor";

    try {
      const response = await fetch(apiUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(requestBody),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(
          errorData.detail || `HTTP error! status: ${response.status}`
        );
      }

      const data = await response.json();

      predictionOutput.textContent = `Prediction: ${
        data.prediction === 1 ? "🎉 It's a HIT! 🎉" : "😞 Not a hit."
      }`;
      modelVersionOutput.textContent = `Model Version: ${data.model_version}`;
      resultContainer.classList.remove("hidden");
    } catch (error) {
      predictionOutput.textContent = `Error: ${error.message}`;
      modelVersionOutput.textContent =
        "Please check the API service and your inputs.";
      resultContainer.classList.remove("hidden");
    }
  });

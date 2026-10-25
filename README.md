# PEMS04 Traffic-Density Prediction

## Project tree

```text
.
├── artifacts/                 # Trained model and metrics
├── data/raw/                  # PEMS04 tensor and sensor-distance graph
├── logs/                      # CLI replay output (created when run)
├── notebooks/
│   └── 01_pems04_data_exploration.ipynb
├── scripts/
│   ├── inspect_data.py
│   ├── run_all.py
│   ├── run_demo.py
│   ├── run_tests.py
│   ├── train_model.py
│   └── verify_stack.py
├── src/traffic_density/
│   ├── cli.py, data.py, demo.py, features.py, pipeline.py
│   └── deploy/                # Kafka, predictor, API, and storage services
├── stack/
│   ├── dashboard.html
│   ├── spark_batch.py
│   ├── spark_speed.py
│   └── sql/
├── tests/
├── run_all.bat, run_all.sh
├── run_demo.bat, run_demo.sh
├── run_stack.bat, run_stack.sh
├── train_model.bat, train_model.sh
├── docker-compose.yml
├── pyproject.toml
├── requirements.txt
├── requirements-notebook.txt
└── requirements-stack.txt
```

## Setup guide

Requirements: Python 3.10 or newer for local scripts. Docker Desktop with Compose v2 is required for the full stack.

Create and activate a virtual environment from the repository root:

```powershell
# Windows PowerShell
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

```bash
# Linux, macOS, or WSL
python3 -m venv .venv
source .venv/bin/activate
```

Install the local package and its dependencies:

```bash
python -m pip install --upgrade pip
python -m pip install -e .
```

Optional notebook dependencies:

```bash
python -m pip install -r requirements-notebook.txt
```

The included dataset files are `data/raw/pems04.npz` and `data/raw/distance.csv`. The Docker image installs the stack dependencies from `requirements-stack.txt` during its build.

## Run script guide

Run these commands from the repository root with `.venv` activated.

| Command | Purpose |
|---|---|
| `python scripts/inspect_data.py` | Print dataset dimensions, time range, and measurement summaries |
| `python scripts/train_model.py --train-ticks 500 --eval-ticks 200` | Train and save a model and metrics under `artifacts/` |
| `python scripts/run_demo.py --ticks 24 --delay 0.2` | Replay readings and write forecasts and logs under `logs/` |
| `python scripts/run_all.py --train-ticks 500 --eval-ticks 200 --ticks 24 --delay 0.2` | Train a model, then run the replay demo |
| `python scripts/run_tests.py` | Run the project test suite |

Start the Docker dashboard stack:

```powershell
# Windows
.\run_stack.bat
```

```bash
# Linux, macOS, or WSL
bash run_stack.sh
```

Or run the equivalent Compose commands directly:

```bash
docker compose up --build -d
python scripts/verify_stack.py --wait-seconds 240 --require-events
```

Open [http://localhost:8000](http://localhost:8000) for the dashboard. It replays historical PEMS04 data, not current roadway telemetry.

For continuous Docker replay from the latest stored step (PowerShell):

```powershell
$env:REPLAY_LOOP="true"
$s=Invoke-RestMethod http://localhost:8000/api/status
$env:REPLAY_START_STEP=([int]$s.last_step + 1).ToString()
$env:REPLAY_DELAY="2"
docker compose up -d --force-recreate producer
```

Stop replay with `docker compose stop producer`.

Run the exploration notebook:

```bash
jupyter lab notebooks/01_pems04_data_exploration.ipynb
```

.PHONY: profile test
profile:
	PYTHONPATH=src python scripts/profile_historical_data.py

test:
	PYTHONPATH=src pytest -q

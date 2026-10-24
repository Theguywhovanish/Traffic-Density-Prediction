.PHONY: profile test validate reference batch inspect

profile:
	python scripts/profile_historical_data.py

test:
	pytest -q

reference:
	python scripts/run_batch_reference.py

batch:
	python scripts/run_batch_lakehouse.py

inspect:
	python scripts/inspect_lakehouse.py

validate:
	python scripts/validate_week2.py

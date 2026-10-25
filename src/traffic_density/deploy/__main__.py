"""Container service entrypoint: python -m traffic_density.deploy SERVICE."""

import argparse
import logging
import os


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("service", choices=["prepare", "producer", "bridge", "predictor"])
    args = parser.parse_args()
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    if args.service == "prepare":
        from .prepare import export_batch_csv

        count = export_batch_csv(os.getenv("DATA_PATH", "/app/data/raw/pems04.npz"),
                                 os.getenv("DISTANCE_PATH", "/app/data/raw/distance.csv"),
                                 os.getenv("BATCH_INPUT", "/shared/batch_input/history.csv"))
        logging.info("SPARK BATCH INPUT READY: %d real training sensor observations", count)
    elif args.service == "producer":
        from .producer import run

        run()
    elif args.service == "bridge":
        from .bridge import run

        run()
    else:
        from .predictor import run

        run()


if __name__ == "__main__":
    main()

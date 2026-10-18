import argparse

from urban_mobility.batch.profile import run


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Profile one Uber Movement CSV or recursively profile a directory of location CSVs."
    )
    parser.add_argument(
        "--input",
        default=None,
        help="CSV file or directory. Defaults to UBER_MOVEMENT_INPUT / data/raw/uber_movement.",
    )
    parser.add_argument(
        "--location",
        default=None,
        help="Optional location override. Only valid when exactly one CSV is processed.",
    )
    args = parser.parse_args()
    run(args.input, location_override=args.location)

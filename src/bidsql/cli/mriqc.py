import argparse
import logging
from pathlib import Path

from bidsql import mapping
from bidsql.a2cps import bids, mriqc

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s  | %(message)s",
    level=logging.INFO,
    force=True,
)


maps = (
    mapping.File.from_str(
        src_pattern=r".*bids_db.*|bidsignore|log\Z|toml\Z",
        parser=mapping.parse_nothing,
    ),
    mapping.File.from_str(
        src_pattern=r"dataset_description\.json\Z",
        parser=bids.parse_dataset,
    ),
    mapping.File.from_str(
        src_pattern=r"sub-\d{5}_ses-V[13].*(dwi|T1w|bold)\.json\Z",
        parser=mriqc.parse_mriqc,
    ),
    mapping.File.from_str(
        src_pattern=r".*",
        parser=bids.parse_file,
    ),
)


def main(root: Path, db: str):
    mapper = mapping.Mapper(
        maps=maps,
        db=db,
        generators=[root.glob("*dataset_description.json"), root.rglob("*")],
    )

    mapper.run()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("db")

    args = parser.parse_args()
    main(root=args.root, db=args.db)

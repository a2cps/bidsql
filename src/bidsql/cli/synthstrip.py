import argparse
import logging
from pathlib import Path

from bidsql import mapping
from bidsql.a2cps import synthstrip

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s  | %(message)s",
    level=logging.INFO,
    force=True,
)

maps = [
    mapping.File.from_str(
        src_pattern=r".*",
        parser=synthstrip.parse_synthstrip_mask,
    )
]


def main(root: Path, db: str):

    generators = []
    for job in root.glob("*/fmriprep/*V[13]/synthstrip"):
        generators.append(job.rglob("*"))

    mapper = mapping.Mapper(maps=maps, db=db, generators=generators)

    mapper.run()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("db")

    args = parser.parse_args()
    main(root=args.root, db=args.db)

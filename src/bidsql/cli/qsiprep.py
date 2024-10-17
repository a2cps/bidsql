import argparse
import logging
from pathlib import Path

from bidsql import mapping
from bidsql.a2cps import bids, qsiprep

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s  | %(message)s",
    level=logging.INFO,
    force=True,
)

maps = (
    mapping.File.from_str(
        src_pattern=r".*sourcedata.*|\.heudiconv|err\Z|out\Z|log\Z",
        parser=mapping.parse_nothing,
    ),
    mapping.File.from_str(
        src_pattern=r"dataset_description\.json\Z",
        parser=bids.parse_dataset,
    ),
    mapping.File.from_str(
        src_pattern=r"sub-\d{5}_ses-V[13]_desc-ImageQC_dwi\.csv\Z",
        parser=qsiprep.parse_qsiprep_imageqc,
    ),
    mapping.File.from_str(
        src_pattern=r"sub-\d{5}.*xfm.*",
        parser=bids.parse_transform,
    ),
    mapping.File.from_str(
        src_pattern=r"sub-\d{5}_ses-V[13]_dwi\.nii\.gz\Z",
        parser=bids.parse_diffusion,
    ),
    mapping.File.from_str(
        src_pattern=r"sub-\d{5}.*_T1w\.nii\.gz\Z",
        parser=bids.parse_anat,
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

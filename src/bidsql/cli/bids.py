import argparse
import logging
from pathlib import Path

from bidsql import mapping
from bidsql.a2cps import bids

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
        src_pattern=r"participants\.tsv\Z",
        parser=bids.parse_participants,
    ),
    mapping.File.from_str(
        src_pattern=r"sessions\.tsv\Z",
        parser=bids.parse_sessions,
    ),
    mapping.File.from_str(
        src_pattern=r"sub-\d{5}_ses-V[13]_scans\.tsv\Z",
        parser=bids.parse_scans,
    ),
    mapping.File.from_str(
        src_pattern=r"sub-\d{5}_ses-V[13]_task-\w+_run-\w+_bold\.nii\.gz\Z",
        parser=bids.parse_func,
    ),
    mapping.File.from_str(
        src_pattern=r"sub-\d{5}_ses-V[13]_dwi\.nii\.gz\Z",
        parser=bids.parse_diffusion,
    ),
    mapping.File.from_str(
        src_pattern=r"sub-\d{5}_ses-V[13]_T1w\.nii\.gz\Z",
        parser=bids.parse_anat,
    ),
    mapping.File.from_str(
        src_pattern=r"sub-\d{5}_ses-V[13].*_epi\.nii\.gz\Z",
        parser=bids.parse_fmap,
    ),
    mapping.File.from_str(
        src_pattern=r".*",
        parser=bids.parse_file,
    ),
)


def main(root: Path, db: str):
    generators = []
    for job in root.glob("*/bids/*V[13]"):
        generators.append(job.glob("*dataset_description.json"))
        generators.append(job.glob("*participants.tsv"))
        for subdir in job.glob("sub*"):
            if subdir.is_dir():
                generators.append(subdir.glob("*sub*sessions.tsv"))

        # add bold and dwi so that fieldmaps can be added later
        for pattern in [
            "*bold.nii.gz",
            "*dwi.nii.gz",
            "*epi.nii.gz",
            "*T1w.nii.gz",  # need T1w explicitly so that *scans.tsv happens after
            "*",
        ]:
            generators.append(job.rglob(pattern))

    mapper = mapping.Mapper(maps=maps, db=db, generators=generators)
    mapper.run()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("db")

    args = parser.parse_args()
    main(root=args.root, db=args.db)

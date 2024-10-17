from pathlib import Path

from sqlalchemy import orm

from bidsql import mapping, models, utils
from bidsql.a2cps import utils as a2cps_utils


def parse_synthstrip_mask(src: Path, session: orm.Session) -> None:
    if mapping.is_file_in_session(src, session):
        return

    dataset = a2cps_utils.get_dataset(src)
    session.add(dataset)

    entities = utils.parse_entities(src)
    participant, ses = mapping.get_add_participant_session(
        session=session,
        participant_id=entities.get("sub"),
        session_id=entities.get("ses"),
    )
    for key in ["ses", "sub", "modality"]:
        entities.pop(key, None)

    session.add(
        models.File(
            dataset=dataset,
            participant=participant,
            session=ses,
            path=str(src.absolute()),
            size=src.stat().st_size,
            mtime=src.stat().st_mtime,
        )
    )

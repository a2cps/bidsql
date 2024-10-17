import json
import typing
from pathlib import Path

from sqlalchemy import orm

from bidsql import mapping, models, utils


def get_iqm_from_json(src: Path) -> dict[str, typing.Any]:
    vals: dict = json.loads(src.read_text())

    return vals


def parse_mriqc(src: Path, session: orm.Session) -> None:
    if mapping.is_file_in_session(src, session):
        return

    entities = utils.parse_entities(src)
    participant, ses = mapping.get_add_participant_session(
        session=session,
        participant_id=entities.get("sub"),
        session_id=entities.get("ses"),
    )
    for key in ["modality", "ses", "sub"]:
        entities.pop(key, None)

    dataset = models.Dataset.from_session(session)

    session.add(
        models.File(
            dataset=dataset,
            participant=participant,
            session=ses,
            extra=get_iqm_from_json(src),
            path=str(src.absolute()),
            size=src.stat().st_size,
            mtime=src.stat().st_mtime,
            **entities,  # type: ignore
        )
    )

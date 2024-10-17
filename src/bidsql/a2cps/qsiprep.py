from pathlib import Path
import typing

import polars as pl
import polars.selectors as cs

from sqlalchemy import orm

from bidsql import models, utils, mapping


def get_iqm(src: Path) -> dict[str, typing.Any]:
    iqms = pl.read_csv(src).drop(cs.ends_with("_id")).drop("file_name").to_dicts()
    if not (nrows := len(iqms)) == 1:
        msg = f"{src} has {nrows} rows but expected 1"
        raise AssertionError(msg)
    return iqms[0]


def parse_qsiprep_imageqc(src: Path, session: orm.Session) -> None:
    if mapping.is_file_in_session(src, session):
        return

    entities = utils.parse_entities(src)
    participant, ses = mapping.get_add_participant_session(
        session=session,
        participant_id=entities.get("sub"),
        session_id=entities.get("ses"),
    )
    for key in ["ses", "sub", "modality"]:
        entities.pop(key, None)

    dataset = models.Dataset.from_session(session)

    session.add(
        models.File(
            dataset=dataset,
            participant=participant,
            session=ses,
            extra=get_iqm(src),
            path=str(src.absolute()),
            size=src.stat().st_size,
            mtime=src.stat().st_mtime,
            **entities,  # type: ignore
        )
    )

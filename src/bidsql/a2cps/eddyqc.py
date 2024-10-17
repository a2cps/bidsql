import json
import typing
from pathlib import Path

from sqlalchemy import orm

from bidsql import mapping, models, utils
from bidsql.a2cps import utils as a2cps_utils


def get_iqm(src: Path) -> dict[str, typing.Any]:
    return json.loads(src.read_text())


def parse_eddyqc_qc(src: Path, session: orm.Session) -> None:
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

    # add both json and pdf so that pdf is not handled by generic
    # parse_file (which would pick up this qc.json as metadata)
    qcjson = models.File(
        dataset=dataset,
        participant=participant,
        session=ses,
        extra=get_iqm(src),
        path=str(src.absolute()),
        size=src.stat().st_size,
        mtime=src.stat().st_mtime,
    )
    pdf = src.absolute().with_suffix(".pdf")
    qcpdf = models.File(
        dataset=dataset,
        participant=participant,
        session=ses,
        path=str(pdf),
        size=pdf.stat().st_size,
        mtime=pdf.stat().st_mtime,
    )
    session.add_all([qcjson, qcpdf])

import logging
from pathlib import Path

import polars as pl
import sqlalchemy as sa
from sqlalchemy import exc, orm

from bidsql import mapping, models, utils
from bidsql.a2cps import utils as converters_utils


def parse_file(src: Path, session: orm.Session) -> None:
    entities = utils.parse_entities(src)
    participant, ses = mapping.get_add_participant_session(
        session=session,
        participant_id=entities.get("sub"),
        session_id=entities.get("ses"),
    )

    for key in ["ses", "sub", "modality", "fmapid"]:
        entities.pop(key, None)

    session.add(
        models.File(
            dataset=models.Dataset.from_session(session),
            participant=participant,
            session=ses,
            modality="file",
            path=str(src.absolute()),
            size=src.stat().st_size,
            mtime=src.stat().st_mtime,
            extra=utils.get_meta_from_path(src),
            **entities,  # type: ignore
        )
    )


def parse_func(src: Path, session: orm.Session) -> None:
    if mapping.is_file_in_session(src, session):
        return

    entities = utils.parse_entities(src)
    participant, ses = mapping.get_add_participant_session(
        session=session,
        participant_id=entities.get("sub"),
        session_id=entities.get("ses"),
    )
    for key in ["ses", "sub"]:
        entities.pop(key, None)

    meta = utils.get_meta_from_path(src)
    path = str(src.absolute())

    dataset = models.Dataset.from_session(session)
    func = models.Func(
        dataset=dataset,
        participant=participant,
        session=ses,
        extra=meta,
        path=path,
        size=src.stat().st_size,
        mtime=src.stat().st_mtime,
        **entities,  # type: ignore
    )

    events = func.read_events()
    session.add_all(*[events, func])


def parse_diffusion(src: Path, session: orm.Session) -> None:
    if mapping.is_file_in_session(src, session):
        return

    entities = utils.parse_entities(src)
    participant, ses = mapping.get_add_participant_session(
        session=session,
        participant_id=entities.get("sub"),
        session_id=entities.get("ses"),
    )
    for key in ["ses", "sub"]:
        entities.pop(key, None)

    meta = utils.get_meta_from_path(src)
    path = str(src.absolute())

    dataset = models.Dataset.from_session(session)

    diffusion = models.Diffusion(
        dataset=dataset,
        participant=participant,
        session=ses,
        extra=meta,
        path=path,
        size=src.stat().st_size,
        mtime=src.stat().st_mtime,
        **entities,  # type: ignore
    )
    btable = models.B.from_dwi(dwi=diffusion)
    session.add_all(*[btable, diffusion])


def parse_anat(src: Path, session: orm.Session) -> None:
    if mapping.is_file_in_session(src, session):
        return

    entities = utils.parse_entities(src)
    participant, ses = mapping.get_add_participant_session(
        session=session,
        participant_id=entities.get("sub"),
        session_id=entities.get("ses"),
    )
    for key in ["ses", "sub"]:
        entities.pop(key, None)

    meta = utils.get_meta_from_path(src)
    path = str(src.absolute())

    dataset = models.Dataset.from_session(session)

    session.add(
        models.Anat(
            dataset=dataset,
            participant=participant,
            session=ses,
            extra=meta,
            path=path,
            size=src.stat().st_size,
            mtime=src.stat().st_mtime,
            **entities,  # type: ignore
        )
    )


def parse_fmap(src: Path, session: orm.Session) -> None:
    if mapping.is_file_in_session(src, session):
        return

    entities = utils.parse_entities(src)
    participant, ses = mapping.get_add_participant_session(
        session=session,
        participant_id=entities.get("sub"),
        session_id=entities.get("ses"),
    )
    for key in ["ses", "sub"]:
        entities.pop(key, None)

    meta = utils.get_meta_from_path(src)
    path = str(src.absolute())

    dataset = models.Dataset.from_session(session)

    files = []
    for ifor in utils.get_ifor_from_niigz(src):
        try:
            files.append(
                models.File.from_pathname_session(ifor, session=session)
            )
        except exc.NoResultFound:
            logging.warning(f"FieldMap target {ifor} not in database")

    session.add(
        models.FieldMap(
            dataset=dataset,
            participant=participant,
            session=ses,
            extra=meta,
            path=path,
            files=files,
            size=src.stat().st_size,
            mtime=src.stat().st_mtime,
            **entities,  # type: ignore
        )
    )


def parse_dataset(src: Path, session: orm.Session) -> None:
    # two scenarios:
    # 1. we're here because we're trying to add a dataset
    # 2. we're trying to add the dataset_description.json file
    #     (and dataset already in session)
    try:
        dataset = models.Dataset.from_session(session=session)
        entities = converters_utils.parse_a2cps_entities(src)
        participant, ses = mapping.get_add_participant_session(
            session=session,
            participant_id=entities.get("sub"),
            session_id=entities.get("ses"),
        )

        session.add(
            models.File(
                dataset=dataset,
                path=str(src.absolute()),
                size=src.stat().st_size,
                mtime=src.stat().st_mtime,
                extension=".json",
                participant=participant,
                session=ses,
            )
        )

    except exc.NoResultFound:
        dataset = models.Dataset.from_file(src)
        session.add(dataset)


def parse_sessions(src: Path, session: orm.Session) -> None:
    sessions_tbl = (
        utils.read_bids_tsv(src)
        .with_columns(
            pl.struct(
                pl.all().exclude(["session_id", "sub", "acquisition_week"])
            ).alias("extra"),
        )
        .select("session_id", "sub", "acquisition_week", "extra")
        .with_columns(
            pl.col("session_id").str.extract(r"(V[13])"),
            pl.col("acquisition_week").str.to_datetime(r"%Y-%m-%d%H:%M:%S"),
        )
    )

    dataset = models.Dataset.from_session(session)

    for ses in sessions_tbl.iter_rows(named=True):
        session.add(
            models.Session(
                dataset=dataset,
                id=utils.get_key_str(ses, "session_id"),
                participant=session.get(
                    models.Participant,
                    {
                        "id": utils.get_key_str(ses, "sub"),
                        "dataset_id": dataset.id,
                    },
                ),
                acq_time=ses.get("acquisition_week"),
                extra=ses.get("extra"),
            )
        )
    parse_file(src=src, session=session)


def parse_participants(src: Path, session: orm.Session) -> None:
    tbl = utils.read_bids_tsv(src)
    participant_column = "sub" if "sub" in tbl.columns else "participant_id"
    toplevel = [participant_column]
    for col in ["age", "sex", "handedness"]:
        if col in tbl.columns:
            toplevel.append(col)

    if "sex" in tbl.columns:
        tbl = tbl.with_columns(
            sex=pl.when(pl.col("sex").is_in(["O", "other", "Other"]))
            .then(pl.lit("other"))
            .when(pl.col("sex").is_in(["M", "male", "Male"]))
            .then(pl.lit("male"))
            .when(pl.col("sex").is_in(["F", "female", "Female"]))
            .then(pl.lit("female"))
        )

    tbl = tbl.with_columns(
        pl.struct(pl.all().exclude(toplevel)).alias("extra"),
        pl.col(participant_column).str.extract(r"(\d{5})"),
    ).select(*toplevel, "extra")

    dataset = models.Dataset.from_session(session)

    for participant in tbl.iter_rows(named=True):
        session.add(
            models.Participant(
                dataset=dataset,
                id=utils.get_key_str(participant, participant_column),
                age=participant.get("age"),
                sex=participant.get("sex"),
                handedness=participant.get("handedness"),
                extra=participant.get("extra"),
            )
        )
    parse_file(src=src, session=session)


def get_file_from_scans_filename(
    filename: str, session: orm.Session
) -> models.File | None:
    return session.scalar(
        sa.select(models.File).where(models.File.path.endswith(filename))
    )


def parse_scans(src: Path, session: orm.Session) -> None:
    scans_tbl = utils.read_bids_tsv(src)
    if "acq_time" in scans_tbl.columns:
        toplevel = ["filename", "acq_time"]
        try:
            scans_tbl = scans_tbl.with_columns(
                pl.col("acq_time").str.to_datetime(r"%F%T"),
            )
        except pl.exceptions.InvalidOperationError:
            scans_tbl = scans_tbl.with_columns(
                pl.col("acq_time").str.to_datetime(r"%FT%T%.6f"),
            )
    else:
        toplevel = ["filename"]

    if not all(col in toplevel for col in scans_tbl.columns):
        scans_tbl = scans_tbl.with_columns(
            pl.struct(pl.all().exclude(toplevel)).alias("extra")
        )

    for scan in scans_tbl.iter_rows(named=True):
        filename = utils.get_key_str(scan, "filename")
        session.add(
            models.Scan(
                filename=filename,
                acq_time=scan.get("acq_time"),
                extra=scan.get("extra"),
                file=get_file_from_scans_filename(filename, session),
            )
        )
    parse_file(src=src, session=session)


def parse_transform(src: Path, session: orm.Session) -> None:
    entities = utils.parse_entities(src)
    participant, ses = mapping.get_add_participant_session(
        session=session,
        participant_id=entities.get("sub"),
        session_id=entities.get("ses"),
    )
    from_id = entities.get("from")
    to_id = entities.get("to")
    for key in ["ses", "sub", "from", "to"]:
        entities.pop(key, None)

    session.add(
        models.Transform(
            dataset=models.Dataset.from_session(session),
            participant=participant,
            session=ses,
            path=str(src.absolute()),
            size=src.stat().st_size,
            mtime=src.stat().st_mtime,
            extra=utils.get_meta_from_path(src),
            from_id=from_id,
            to_id=to_id,
            **entities,  # type: ignore
        )
    )

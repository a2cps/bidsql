import logging
import re
import typing
from collections import abc
from pathlib import Path

import pydantic
import sqlalchemy as sa
from sqlalchemy import exc, orm

from bidsql import models

type Parser = typing.Callable[[Path, orm.Session], None]


class File(pydantic.BaseModel):
    pattern: re.Pattern
    parser: Parser

    @classmethod
    def from_str(cls, src_pattern: str, parser: Parser) -> typing.Self:
        return cls(pattern=re.compile(src_pattern), parser=parser)

    def to_model(self, src: Path, session: orm.Session) -> None:
        return self.parser(src, session)


class Mapper(pydantic.BaseModel):
    maps: typing.Sequence[File]
    generators: typing.Sequence[abc.Generator[Path, None, None]]
    db: str

    def run(self) -> None:
        engine = sa.create_engine(self.db)
        models.Base.metadata.create_all(engine)
        with orm.Session(engine) as session:
            for generator in self.generators:
                for file in generator:
                    if file.is_dir():
                        continue

                    attempt_map(file, self.maps, session=session)

            # now remove from the database anything referring to a file that no longer exists
            for file in session.scalars(sa.select(models.File.path)).all():
                if not Path(file).exists():
                    logging.info(f"Deleting {file} from database")
                    session.delete(file)

            session.commit()


def parse_nothing(src: Path, _: orm.Session) -> None:
    logging.info(f"Skipping {src}")


def is_file_in_session(src: Path, session: orm.Session) -> bool:
    try:
        if (
            existing_file := models.File.from_path_session(src, session)
        ) and existing_file.mtime == src.stat().st_mtime:
            is_in = True
        else:
            is_in = False
    except exc.NoResultFound:
        is_in = False
    return is_in


def attempt_map(
    src: Path, incoming_to_natives: typing.Sequence[File], session: orm.Session
) -> None:
    if is_file_in_session(src=src, session=session):
        logging.info(f"{src} already in database")
        return

    for mapping in incoming_to_natives:
        if mapping.pattern.search(str(src)):
            logging.info(f"Adding {src} with {mapping.parser.__name__}")
            return mapping.to_model(src, session=session)

    logging.warning(f"Did not find parser for {src}")


def get_add_participant_session(
    session: orm.Session,
    participant_id: str | None = None,
    session_id: str | None = None,
) -> tuple[models.Participant | None, models.Session | None]:
    dataset = models.Dataset.from_session(session)
    participant = (
        upsert_participant(session, id=participant_id, dataset=dataset)
        if participant_id
        else None
    )
    if participant and session_id:
        ses = upsert_session(
            session,
            id=session_id,
            dataset=dataset,
            participant=participant,
        )
    else:
        ses = None

    return participant, ses


def upsert_participant(
    session: orm.Session, id: str, dataset: models.Dataset
) -> models.Participant:
    try:
        participant = models.Participant.from_session(session, id=id)
    except exc.NoResultFound:
        logging.info(
            f"Unable to find participant {id} in session; attempting to add"
        )
        participant = models.Participant(id=id, dataset=dataset)
        session.add(participant)
    return participant


def upsert_session(
    session: orm.Session,
    id: str,
    participant: models.Participant,
    dataset: models.Dataset,
) -> models.Session:
    try:
        ses = models.Session.from_session(
            session, id=id, participant_id=participant.id
        )
    except Exception:
        logging.info(
            f"Unable to find session {id} in session; attempting to add"
        )
        ses = models.Session(id=id, dataset=dataset, participant=participant)
        session.add(ses)
    return ses

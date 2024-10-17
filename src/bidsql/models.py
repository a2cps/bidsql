import json
import typing
import uuid
from datetime import datetime
from pathlib import Path

import polars as pl
import sqlalchemy as sa
from sqlalchemy import orm

from bidsql import fields


class Base(orm.MappedAsDataclass, orm.DeclarativeBase):
    pass


class Dataset(Base):
    __tablename__ = "dataset"

    name: orm.Mapped[str | None] = orm.mapped_column(sa.String)
    bids_version: orm.Mapped[str | None] = orm.mapped_column(sa.String)

    participants: orm.Mapped[list["Participant"]] = orm.relationship(
        back_populates="dataset",
        cascade="all, delete-orphan",
        passive_deletes=True,
        default_factory=list,
    )
    sessions: orm.Mapped[list["Session"]] = orm.relationship(
        back_populates="dataset",
        cascade="all, delete-orphan",
        passive_deletes=True,
        default_factory=list,
    )
    files: orm.Mapped[list["File"]] = orm.relationship(
        back_populates="dataset",
        cascade="all, delete-orphan",
        passive_deletes=True,
        default_factory=list,
    )
    id: orm.Mapped[uuid.UUID] = orm.mapped_column(primary_key=True, default_factory=uuid.uuid4, init=False)

    @classmethod
    def from_session(cls, session: orm.Session) -> typing.Self:
        dataset = session.scalars(sa.select(cls)).one()
        return dataset

    @classmethod
    def from_file(cls, src: Path) -> typing.Self:
        description: dict[str, str] = json.loads(src.read_text())
        return cls(
            name=description.get("Name"),
            bids_version=description.get("BIDSVersion"),
        )


class Participant(Base):
    __tablename__ = "participant"

    id: orm.Mapped[str] = orm.mapped_column(primary_key=True)
    dataset_id: orm.Mapped[str] = orm.mapped_column(
        sa.ForeignKey("dataset.id", ondelete="CASCADE"),
        primary_key=True,
        default=None,
    )
    sex: orm.Mapped[fields.Sex | None] = orm.mapped_column(sa.Enum(*typing.get_args(fields.Sex)), default=None)
    age: orm.Mapped[int | None] = orm.mapped_column(sa.SmallInteger, default=None)
    handedness: orm.Mapped[int | None] = orm.mapped_column(sa.Enum(*typing.get_args(fields.Handedness)), default=None)
    extra: orm.Mapped[dict | None] = orm.mapped_column(sa.JSON, default_factory=sa.null)

    dataset: orm.Mapped[Dataset | None] = orm.relationship(back_populates="participants", default=None)
    sessions: orm.Mapped[list["Session"] | None] = orm.relationship(
        back_populates="participant",
        cascade="all, delete-orphan",
        default_factory=list,
        passive_deletes=True,
    )
    files: orm.Mapped[list["File"] | None] = orm.relationship(
        back_populates="participant",
        cascade="all, delete-orphan",
        passive_deletes=True,
        default_factory=list,
    )

    @classmethod
    def from_session(cls, session: orm.Session, id: str) -> typing.Self:
        return session.scalars(sa.select(cls).where(cls.id == id)).one()


class Session(Base):
    """_summary_

    Args:
        Base (_type_): _description_

    Raises:
        RuntimeError: _description_

    Returns:
        _type_: _description_

    Details:
        Session must know about participant, because the sessions
        file may contain info unique to each participant
    """

    __tablename__ = "session"

    id: orm.Mapped[str] = orm.mapped_column(primary_key=True)
    participant_id: orm.Mapped[str] = orm.mapped_column(
        sa.ForeignKey("participant.id", ondelete="CASCADE"),
        primary_key=True,
        default=None,
    )
    dataset_id: orm.Mapped[str] = orm.mapped_column(
        sa.ForeignKey("dataset.id", ondelete="CASCADE"),
        primary_key=True,
        default=None,
    )
    acq_time: orm.Mapped[datetime | None] = orm.mapped_column(sa.DATETIME, default=None)
    extra: orm.Mapped[dict | None] = orm.mapped_column(sa.JSON, default_factory=sa.null)

    dataset: orm.Mapped[Dataset | None] = orm.relationship(back_populates="sessions", default=None)
    participant: orm.Mapped[Participant | None] = orm.relationship(back_populates="sessions", default=None)
    files: orm.Mapped[list["File"]] = orm.relationship(
        back_populates="session",
        cascade="all, delete-orphan",
        passive_deletes=True,
        default_factory=list,
    )

    @classmethod
    def from_session(cls, session: orm.Session, id: str, participant_id: str) -> typing.Self:
        ses = session.scalar(sa.select(cls).where(cls.id == id).where(cls.participant_id == participant_id))
        if not isinstance(ses, cls):
            msg = f"Retrieved unexpected object: {ses=}"
            raise RuntimeError(msg)
        return ses


fieldmap_file_link = sa.Table(
    "fieldmap_file_link",
    Base.metadata,
    sa.Column(
        "fieldmap_path",
        sa.ForeignKey("fieldmap.file_path", ondelete="CASCADE"),
        primary_key=True,
    ),
    sa.Column(
        "file_path",
        sa.ForeignKey("file.path", ondelete="CASCADE"),
        primary_key=True,
    ),
)


class File(Base):
    __tablename__ = "file"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {
        "polymorphic_on": "modality",
        "polymorphic_identity": "file",
    }

    path: orm.Mapped[str] = orm.mapped_column(primary_key=True)

    dataset_id: orm.Mapped[str] = orm.mapped_column(sa.ForeignKey("dataset.id", ondelete="CASCADE"), default=None)
    participant_id: orm.Mapped[str | None] = orm.mapped_column(
        sa.ForeignKey("participant.id", ondelete="CASCADE"), default=None
    )
    session_id: orm.Mapped[str | None] = orm.mapped_column(
        sa.ForeignKey("session.id", ondelete="CASCADE"), default=None
    )
    size: orm.Mapped[int | None] = orm.mapped_column(default=None)
    mtime: orm.Mapped[float | None] = orm.mapped_column(default=None)
    acq: orm.Mapped[str | None] = orm.mapped_column(default=None)
    dir: orm.Mapped[str | None] = orm.mapped_column(default=None)
    run: orm.Mapped[str | None] = orm.mapped_column(default=None)
    space: orm.Mapped[str | None] = orm.mapped_column(default=None)
    den: orm.Mapped[str | None] = orm.mapped_column(default=None)
    res: orm.Mapped[str | None] = orm.mapped_column(default=None)
    hemi: orm.Mapped[fields.Hemi | None] = orm.mapped_column(sa.Enum(*typing.get_args(fields.Hemi)), default=None)
    bval: orm.Mapped[str | None] = orm.mapped_column(default=None)
    task: orm.Mapped[str | None] = orm.mapped_column(default=None)
    desc: orm.Mapped[str | None] = orm.mapped_column(default=None)
    label: orm.Mapped[str | None] = orm.mapped_column(default=None)
    modality: orm.Mapped[str] = orm.mapped_column(default="file")
    suffix: orm.Mapped[str | None] = orm.mapped_column(default=None)
    extension: orm.Mapped[str | None] = orm.mapped_column(default=None)

    extra: orm.Mapped[dict | None] = orm.mapped_column(sa.JSON, default_factory=sa.null)
    dataset: orm.Mapped[Dataset | None] = orm.relationship(back_populates="files", default=None)
    participant: orm.Mapped[Participant | None] = orm.relationship(back_populates="files", default=None)

    # for details on primaryjoin, see
    # https://docs.sqlalchemy.org/en/20/orm/join_conditions.html#overlapping-foreign-keys
    session: orm.Mapped[Session | None] = orm.relationship(
        back_populates="files",
        default=None,
        primaryjoin="and_(Session.id == foreign(File.session_id), "
        "Session.participant_id == File.participant_id, "
        "Session.dataset_id == File.dataset_id)",
    )
    fieldmaps: orm.Mapped[list["FieldMap"] | None] = orm.relationship(
        back_populates="files",
        secondary=fieldmap_file_link,
        default_factory=list,
    )
    scan: orm.Mapped[typing.Optional["Scan"]] = orm.relationship(
        back_populates="file",
        default=None,
    )

    @classmethod
    def from_path_session(cls, src: Path, session: orm.Session) -> typing.Self:
        return session.scalars(sa.select(cls).where(cls.path == str(src.absolute()))).one()

    @classmethod
    def from_pathname_session(cls, src: Path, session: orm.Session) -> typing.Self:
        return session.scalars(sa.select(cls).where(cls.path.contains(src.name))).one()


class FilePathMixin(orm.MappedAsDataclass):
    file_path: orm.Mapped[str] = orm.mapped_column(
        sa.ForeignKey("file.path"),
        primary_key=True,
        default=None,
    )


class Scan(FilePathMixin, Base):
    __tablename__ = "scan"

    filename: orm.Mapped[str | None] = orm.mapped_column(default=None)
    acq_time: orm.Mapped[datetime | None] = orm.mapped_column(sa.DATETIME, default=None)
    extra: orm.Mapped[dict | None] = orm.mapped_column(sa.JSON, default_factory=sa.null)
    file: orm.Mapped[File | None] = orm.relationship(back_populates="scan", default=None)


class Anat(FilePathMixin, File):
    __tablename__ = "anat"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {
        "polymorphic_identity": "anat",
    }


class Event(Base):
    __tablename__ = "event"

    onset: orm.Mapped[float]
    duration: orm.Mapped[float]
    extra: orm.Mapped[dict | None] = orm.mapped_column(sa.JSON, default_factory=sa.null)
    func_path: orm.Mapped[str] = orm.mapped_column(sa.ForeignKey("func.file_path"), primary_key=True, default=None)
    func: orm.Mapped[typing.Optional["Func"]] = orm.relationship(back_populates="events", default=None)
    id: orm.Mapped[uuid.UUID] = orm.mapped_column(primary_key=True, default_factory=uuid.uuid4)


class FieldMap(FilePathMixin, File):
    __tablename__ = "fieldmap"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {
        "polymorphic_identity": "fmap",
    }

    files: orm.Mapped[list["File"] | None] = orm.relationship(
        back_populates="fieldmaps",
        secondary=fieldmap_file_link,
        default_factory=list,
    )


class Func(FilePathMixin, File):
    __tablename__ = "func"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {
        "polymorphic_identity": "func",
    }

    events: orm.Mapped[list[Event] | None] = orm.relationship(back_populates="func", default_factory=list)

    def read_events(self) -> list[Event]:
        path = Path(self.path)
        event_path = path.parent / path.name.replace("bold.nii.gz", "events.tsv")
        events_to_add: list[Event] = []
        if event_path.exists():
            events = (
                pl.read_csv(event_path, separator="\t")
                .with_columns(pl.struct(pl.all().exclude(["onset", "duration"])).alias("extra"))
                .select("onset", "duration", "extra")
            )
            for event in events.iter_rows(named=True):
                events_to_add.append(Event(func=self, **event))

        return events_to_add


class B(Base):
    __tablename__ = "bvalbvec"

    tr: orm.Mapped[int] = orm.mapped_column(primary_key=True)
    b: orm.Mapped[int]
    x: orm.Mapped[int]
    y: orm.Mapped[int]
    z: orm.Mapped[int]

    diffusion_path: orm.Mapped[str] = orm.mapped_column(
        sa.ForeignKey("diffusion.file_path"), primary_key=True, default=None
    )
    dwi: orm.Mapped[typing.Optional["Diffusion"]] = orm.relationship(back_populates="bvalbvecs", default=None)

    @classmethod
    def from_json(cls, dwi: "Diffusion", dwi_meta: Path) -> list[typing.Self]:
        if (bval_path := dwi_meta.with_suffix(".bval")).exists():
            bvals = [float(bval) for bval in bval_path.read_text().split()]
        if (bvec_path := dwi_meta.with_suffix(".bvec")).exists():
            bvecs = [[float(v) for v in line.split()] for line in bvec_path.read_text().splitlines()]
        df = pl.DataFrame({"b": bvals, "x": bvecs[0], "y": bvecs[1], "z": bvecs[2]}).with_row_index(name="tr")

        out: list[typing.Self] = []
        for row in df.iter_rows(named=True):
            out.append(cls(**row, dwi=dwi, diffusion_path=dwi.path))
        return out

    @classmethod
    def from_dwi(cls, dwi: "Diffusion") -> list[typing.Self]:
        meta = Path(dwi.path.removesuffix(".gz")).with_suffix(".json")
        return cls.from_json(dwi=dwi, dwi_meta=meta)


class Diffusion(FilePathMixin, File):
    __tablename__ = "diffusion"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {
        "polymorphic_identity": "dwi",
    }

    bvalbvecs: orm.Mapped[list[B] | None] = orm.relationship(back_populates="dwi", default_factory=list)


class Transform(FilePathMixin, File):
    __tablename__ = "transform"
    __mapper_args__: typing.ClassVar[dict[str, typing.Any]] = {
        "polymorphic_identity": "xfm",
    }

    from_id: orm.Mapped[str | None] = orm.mapped_column(default=None)
    to_id: orm.Mapped[str | None] = orm.mapped_column(default=None)
    mode: orm.Mapped[str | None] = orm.mapped_column(default=None)

import json
import typing
import re
from pathlib import Path

import sqlalchemy as sa
import polars as pl


def parse_entity(src: str, entity: str) -> str | None:
    check = re.search(f"(?<={entity}-)([a-zA-Z0-9]+)", src)
    return check.group() if check is not None else None


def remove_niigz(src: Path) -> Path:
    return Path(str(src).removesuffix(".gz").removesuffix(".nii"))


def parse_extension(src: Path) -> str:
    if src.name.endswith(".nii.gz"):
        extension = ".nii.gz"
    elif src.name.endswith(".surf.gii"):
        extension = ".surf.gii"
    elif src.name.endswith(".dtseries.nii"):
        extension = ".dtseries.nii"
    else:
        extension = src.suffix

    return extension


def parse_modality(src: Path) -> str:
    if "xfm" in str(src):
        modality = "xfm"
    elif "/anat/" in str(src):
        modality = "anat"
    elif "/dwi/" in str(src):
        modality = "dwi"
    elif "/fmap/" in str(src):
        modality = "fmap"
    elif "/func/" in str(src):
        modality = "func"
    elif "/figures/" in str(src):
        modality = "figures"
    else:
        modality = "file"

    return modality


# NOTE: this relies on a2cps-specific formatting
def parse_entities(src: Path) -> dict[str, str]:
    extension = parse_extension(src)
    parts = src.name.split("_")
    suffix = parts[-1].removesuffix(extension)
    modality = parse_modality(src)
    entities = {"suffix": suffix, "extension": extension, "modality": modality}
    for part in parts[:-1]:
        split = part.split("-")
        if len(split) == 2:
            entities.update({split[0]: split[1]})

    return entities


def get_key_str(entities: dict[str, typing.Any], key: str) -> str:
    value = entities.get(key)
    if not isinstance(value, str):
        msg = f"entities does not have str {key=}? {entities}"
        raise RuntimeError(msg)
    return value


def get_meta_from_path(src: Path) -> dict[str, typing.Any] | sa.Null:
    entities = parse_entities(src)
    extension = entities.get("extension")
    if not entities.get("extension") or not isinstance(extension, str):
        msg = f"Unable to parse extension in {src}, so unable to look for sidecar"
        raise RuntimeError(msg)

    sidecar = Path(str(src).replace(extension, ".json"))

    # need to consider case where this function was called on a sidecar
    if (sidecar == src) or not sidecar.exists():
        return sa.null()

    meta: dict[str, typing.Any] = json.loads(sidecar.read_text())
    meta.pop("global", None)
    return meta


def get_ifor_from_niigz(niigz: Path) -> list[Path]:
    meta = get_meta_from_path(niigz)
    ifors: list[str] = meta.get("IntendedFor", []) if isinstance(meta, dict) else []
    return [Path(src) for src in ifors]


def read_bids_tsv(src: Path) -> pl.DataFrame:
    df = pl.read_csv(src, separator="\t", null_values="n/a")
    if "sub" in df.columns:
        df = df.with_columns(pl.col("sub").cast(pl.Utf8))

    return df

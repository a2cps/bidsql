from pathlib import Path
import re

from bidsql import utils, models


def search_for_entity(src: Path, pattern: str) -> str | None:
    maybe = re.findall(pattern, str(src))
    if len(maybe):
        return maybe[0]


def search_for_sub(src: Path) -> str | None:
    return search_for_entity(src, r"\d{5}(?=V)")


def search_for_ses(src: Path) -> str | None:
    return search_for_entity(src, r"V[13]")


def parse_a2cps_entities(src: Path) -> dict[str, str]:
    entities = utils.parse_entities(src)

    if not entities.get("sub") and (participant_id := search_for_sub(src)):
        entities["sub"] = participant_id

    if not entities.get("ses") and (session_id := search_for_ses(src)):
        entities["ses"] = session_id

    return entities


def get_dataset_name(src: Path) -> str:
    name = re.findall(r"\w{2}\d{5}V[13]", str(src))
    if not len(name) == 1:
        msg = f"Unable to extract name from {src}"
        raise AssertionError(msg)
    return name[0]


def get_dataset(src: Path) -> models.Dataset:
    dsname = get_dataset_name(src)
    dataset = models.Dataset(bids_version="", name=dsname)
    return dataset

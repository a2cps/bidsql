import re
from pathlib import Path

import pandas as pd
from sqlalchemy import orm

from bidsql import mapping, models, utils
from bidsql.a2cps import utils as a2cps_utils


def _get_int(line: str) -> int:
    return int(re.findall(r"\d+", line)[0])


def _get_float(line: str) -> float:
    return float(re.findall(r"\d+.\d+", line)[0])


def parse_aparc(f: Path) -> pd.DataFrame:
    return pd.read_csv(
        f,
        delim_whitespace=True,
        comment="#",
        names=[
            "StructName",
            "NumVert",
            "SurfArea",
            "GrayVol",
            "ThickAvg",
            "ThickStd",
            "MeanCurv",
            "GausCurv",
            "FoldInd",
            "CurvInd",
        ],
    )


def _parse_aseg_header(f: Path) -> pd.DataFrame:
    dfs = []
    lines = f.read_text().splitlines()
    for line in lines:
        if "Measure BrainSeg, BrainSegVol" in line:
            dfs.append(pd.DataFrame({"BrainSegVol": [_get_float(line)]}))
        elif "Measure BrainSegNotVent, BrainSegVolNotVent" in line:
            dfs.append(pd.DataFrame({"BrainSegVolNotVent": [_get_float(line)]}))
        elif "Measure BrainSegNotVentSurf, BrainSegVolNotVentSurf" in line:
            dfs.append(pd.DataFrame({"BrainSegVolNotVentSurf": [_get_float(line)]}))
        elif "Measure Cortex, CortexVol" in line:
            dfs.append(pd.DataFrame({"CortexVol": [_get_float(line)]}))
        elif "Measure SupraTentorial, SupraTentorialVol" in line:
            dfs.append(pd.DataFrame({"SupraTentorialVol": [_get_float(line)]}))
        elif "Measure SupraTentorialNotVent, SupraTentorialVolNotVent" in line:
            dfs.append(pd.DataFrame({"SupraTentorialVolNotVent": [_get_float(line)]}))
        elif "Measure EstimatedTotalIntraCranialVol, eTIV" in line:
            dfs.append(pd.DataFrame({"eTIV": [_get_float(line)]}))
        elif "Measure VentricleChoroidVol, VentricleChoroidVol" in line:
            dfs.append(pd.DataFrame({"VentricleChoroidVol": [_get_float(line)]}))
        elif "Measure lhCortex, lhCortexVol" in line:
            dfs.append(pd.DataFrame({"lhCortexVol": [_get_float(line)]}))
        elif "Measure rhCortex, rhCortexVol" in line:
            dfs.append(pd.DataFrame({"rhCortexVol": [_get_float(line)]}))
        elif "Measure lhCerebralWhiteMatter, lhCerebralWhiteMatterVol" in line:
            dfs.append(pd.DataFrame({"lhCerebralWhiteMatterVol": [_get_float(line)]}))
        elif "Measure rhCerebralWhiteMatter, rhCerebralWhiteMatterVol" in line:
            dfs.append(pd.DataFrame({"rhCerebralWhiteMatterVol": [_get_float(line)]}))
        elif "Measure CerebralWhiteMatter, CerebralWhiteMatterVol" in line:
            dfs.append(pd.DataFrame({"CerebralWhiteMatterVol": [_get_float(line)]}))
        elif "Measure SubCortGray, SubCortGrayVol" in line:
            dfs.append(pd.DataFrame({"SubCortGrayVol": [_get_float(line)]}))
        elif "Measure TotalGray, TotalGrayVol" in line:
            dfs.append(pd.DataFrame({"TotalGrayVol": [_get_float(line)]}))
        elif "Measure SupraTentorialNotVentVox, SupraTentorialVolNotVentVox" in line:
            dfs.append(pd.DataFrame({"SupraTentorialVolNotVentVox": [_get_float(line)]}))
        elif "Measure Mask, MaskVol" in line:
            dfs.append(pd.DataFrame({"MaskVol": [_get_float(line)]}))
        elif "BrainSegVol-to-eTIV, BrainSegVol-to-eTIV" in line:
            dfs.append(pd.DataFrame({"BrainSegVol-to-eTIV": [_get_float(line)]}))
        elif "MaskVol-to-eTIV" in line:
            dfs.append(pd.DataFrame({"Mask-to-eTIV": [_get_float(line)]}))
        elif "lhSurfaceHoles" in line:
            dfs.append(pd.DataFrame({"lhSurfaceHoles": [_get_int(line)]}))
        elif "rhSurfaceHoles" in line:
            dfs.append(pd.DataFrame({"rhSurfaceHoles": [_get_int(line)]}))
        elif "SurfaceHoles, SurfaceHoles" in line:
            dfs.append(pd.DataFrame({"SurfaceHoles": [_get_int(line)]}))

    return pd.concat(dfs, axis=1).reset_index(drop=True)


def _parse_aparc_header(f: Path) -> pd.DataFrame:
    dfs = []

    lines = f.read_text().splitlines()
    for line in lines:
        if "Measure Cortex, NumVert" in line:
            dfs.append(pd.DataFrame({"NumVert": [_get_int(line)]}))
        elif "Measure Cortex, WhiteSurfArea" in line:
            dfs.append(pd.DataFrame({"WhiteSurfArea": [_get_float(line)]}))
        elif "Measure Cortex, MeanThickness" in line:
            dfs.append(pd.DataFrame({"MeanThickness": [_get_float(line)]}))
    return pd.concat(dfs, axis=1).reset_index(drop=True)


def parse_all_headers(root: Path) -> pd.DataFrame:
    headers = []
    for subsesdir in root.glob("sub*"):
        aseg = _parse_aseg_header(subsesdir / "stats" / "aseg.stats")
        aparc = _parse_aparc_header(subsesdir / "stats" / "lh.aparc.stats")
        headers.append(pd.concat([aseg, aparc]))

    return pd.concat(headers).reset_index(drop=True)


def parse_aseg(f: Path) -> pd.DataFrame:
    d = pd.read_csv(
        f,
        delim_whitespace=True,
        comment="#",
        names=[
            "Index",
            "SegId",
            "NVoxels",
            "Volume_mm3",
            "StructName",
            "normMean",
            "normStdDev",
            "normMin",
            "normMax",
            "normRange",
        ],
    )
    return d


def parse_all_aparc(root: Path) -> pd.DataFrame:
    aparc = []
    for subsesdir in root.glob("sub*"):
        for hemi in ["lh", "rh"]:
            for parc in [
                "aparc",
                "aparc.pial",
                "BA_exvivo",
                "BA_exvivo.thresh",
                "aparc.DKTatlas",
                "aparc.a2009s",
            ]:
                aparc.append(
                    parse_aparc(subsesdir / "stats" / f"{hemi}.{parc}.stats").assign(hemisphere=hemi, parc=parc)
                )

    return pd.concat(aparc, ignore_index=True)


def parse_all_aseg(root: Path) -> pd.DataFrame:
    aseg = []
    for subsesdir in root.glob("sub*"):
        aseg.append(parse_aseg(subsesdir / "stats" / "aseg.stats").assign(seg="aseg"))
        aseg.append(parse_aseg(subsesdir / "stats" / "wmparc.stats").assign(seg="wmparc"))

    return pd.concat(aseg, ignore_index=True)


def parse_freesurfer(src: Path, session: orm.Session) -> None:
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

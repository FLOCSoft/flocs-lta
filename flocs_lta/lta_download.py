#!/usr/bin/env python
import casacore.tables as ct
import os
import shutil
import structlog
from concurrent.futures import ProcessPoolExecutor
from enum import Enum
from typing import Iterable, Optional

logger = structlog.getLogger()


def is_valid_ms(ms_path: str):
    """Performs a couple of checks to verify that the MS is intact. Adapted from T. Shimwell.

    Args:
        ms_path (str): path to the MS to verify.

    Returns:
        bool: False if any of the checks fail; True otherwise.

    Raises:
        ValueError: if the path is not a directory.
    """
    if not os.path.isdir(ms_path):
        raise ValueError(f"{ms_path} is not a directory.")

    required_subtables = (
        "ANTENNA",
        "FIELD",
        "OBSERVATION",
        "SPECTRAL_WINDOW",
        "POLARIZATION",
        "DATA_DESCRIPTION",
    )

    is_valid = True

    try:
        with ct.table(ms_path, readonly=True, ack=False) as tab:
            nrows = tab.nrows()
            if not nrows:
                print(f"{ms_path} contains no rows.")
                is_valid = False
            cols = set(tab.colnames())

            for rt in required_subtables:
                if rt not in tab.fieldnames():
                    print(f"Required table {rt} not found in MS.")
                    is_valid = False
                # Touch one data cell where possible to catch truncated payloads
                # that still allow table metadata to open.
                for dcol in ("DATA", "CORRECTED_DATA", "FLOAT_DATA"):
                    if dcol in cols:
                        tab.getcell(dcol, 0)
                        break

                for sub in required_subtables:
                    sub_path = f"{ms_path.rstrip("/")}::{sub}"
                    with tab.table(sub_path, readonly=True, ack=False) as st:
                        if not st.nrows():
                            print(f"Sub table {sub} has no rows.")
                            is_valid = False
                        # Read one representative value for lightweight content validation.
                        sub_cols = set(st.colnames())
                        if sub == "FIELD" and "PHASE_DIR" in sub_cols:
                            st.getcell("PHASE_DIR", 0)
                        elif sub == "ANTENNA" and "NAME" in sub_cols:
                            st.getcell("NAME", 0)
    except (RuntimeError, AttributeError):
        is_valid = False

    return is_valid


class LTASite(Enum):
    JUELICH = "Juelich"
    POZNAN = "Poznan"
    SURF = "SURF"


class Downloader:
    def __init__(self, urls: Iterable, macaroons: dict):
        """Initialise a Downloader object.

        Args:
            urls (list): a list (or other iterable) of URLs to download.
            macaroons (dict): dictionary with maracoons for the various LTA sites.
        """
        self.macaroons = macaroons
        self.urls = urls

    def download_url(
        self,
        arguments: tuple[str, bool, str],
    ):
        """Download the MS pointed to by the URL.

        Args:
            url (str): URL to download.
            outdir (str): directory to put downloaded files in.

        Raises:
            RuntimeError: when encountering an unknown LTA site.
        """
        url, extract, outdir = arguments
        site = None
        sasid = url.split("/")[-1].split("_")[0]
        outdir_full = os.path.join(os.path.abspath(outdir), sasid)
        try:
            os.makedirs(outdir_full)
        except FileExistsError:
            pass
        outname = os.path.join(outdir_full, url.split("/")[-1])
        if "juelich" in url:
            site = LTASite.JUELICH
        elif "psnc" in url:
            site = LTASite.POZNAN
        elif "surf" in url:
            site = LTASite.SURF
        if not site:
            raise RuntimeError("Unknown LTA site encountered.")
        # strip the hash + tar extension
        ms = outname.split("MS")[0] + "MS"
        if not os.path.isdir(ms):
            print(
                f"wget --no-clobber --retry-on-http-error 401,500 --check-certificate=off {url}?authz={self.macaroons[site.value]} -O {outname}"
            )
            os.system(
                f"wget --no-clobber --retry-on-http-error 401,500 --check-certificate=off {url}?authz={self.macaroons[site.value]} -O {outname}"
            )
            if extract:
                import tarfile

                print(f"Extracting {outname}")
                with tarfile.open(outname, "r") as tarball:
                    tarball.extractall(path=outdir_full)

                valid = is_valid_ms(ms)
                if not valid:
                    print(f"{ms} is not a valid MS, removing it.")
                    shutil.rmtree(ms)

                try:
                    with ct.table(ms) as tab:
                        has_dysco = (
                            tab.getdesc()["DATA"]["dataManagerGroup"] == "DyscoData"
                        ) | (tab.getdesc()["DATA"]["dataManagerType"] == "DyscoStMan")
                    if has_dysco:
                        logger.info(
                            f"{ms} is already dysco compressed. Deleting archive."
                        )
                        os.remove(outname)
                    else:
                        logger.info(
                            f"{ms} is not dysco compressed, compressing with DP3."
                        )
                        if "CWL_SINGULARITY_CACHE" in os.environ:
                            os.rename(ms, ms + ".nodysco")
                            os.system(
                                f"apptainer exec {os.path.join(os.environ['CWL_SINGULARITY_CACHE'], 'astronrd_linc_latest.sif')} DP3 numthreads=2 msin={ms+'.nodysco'} msout={ms} msout.storagemanager=dysco steps=[]"
                            )
                            os.remove(outname)
                            shutil.rmtree(ms + ".nodysco")
                        elif shutil.which("DP3"):
                            os.rename(ms, ms + ".nodysco")
                            os.system(
                                f"DP3 msin={ms+'.nodysco'} msout={ms} msout.storagemanager=dysco steps=[]"
                            )
                            os.remove(outname)
                            shutil.rmtree(ms + ".nodysco")
                except:
                    print(f"Failed to Dysco compress {ms}.")
        else:
            print(f"{ms} already exists.")

    def download_all(
        self,
        max_workers: Optional[int] = 1,
        extract: Optional[bool] = False,
        outdir: Optional[str] = os.getcwd(),
    ):
        """Download all URLs belonging to the instance.

        Args:
            max_workers (int):
        """
        with ProcessPoolExecutor(max_workers=max_workers) as pex:
            pex.map(
                self.download_url,
                [(url, extract, outdir) for url in self.urls],
            )

#!/usr/bin/env python
import os
import shutil
from concurrent.futures import ProcessPoolExecutor
from enum import Enum
from typing import Iterable, Optional



class LTASite(Enum):
    JUELICH = "Juelich"
    POZNAN = "Poznan"
    SURF = "SURF"


class VerificationLevel(Enum):
    BASIC = "basic"


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
        arguments: tuple[str, bool, str, str],
    ):
        """Download the MS pointed to by the URL.

        Args:
            url (str): URL to download.
            outdir (str): directory to put downloaded files in.

        Raises:
            RuntimeError: when encountering an unknown LTA site.
        """
        url, extract, verification, outdir = arguments
        site = None
        sasid = url.split("/")[-1].split("_")[0]
        outdir_full = os.path.join(os.path.abspath(outdir), sasid)
        try:
            os.mkdir(outdir_full)
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

                if verification == "basic":
                    import casacore.tables as ct

                    try:
                        with ct.table(ms) as tab:
                            has_dysco = (
                                tab.getdesc()["DATA"]["dataManagerGroup"] == "DyscoData"
                            )
                        if has_dysco:
                            os.remove(outname)
                        else:
                            if "CWL_SINGULARITY_CACHE" in os.environ.keys():
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
                        print(f"{ms} is not a valid MeasurementSet")
        else:
            print(f"{ms} already exists.")

    def download_all(
        self,
        max_workers: Optional[int] = 1,
        extract: Optional[bool] = False,
        verification: Optional[str] = "basic",
        outdir: Optional[str] = os.getcwd(),
    ):
        """Download all URLs belonging to the instance.

        Args:
            max_workers (int):
        """
        with ProcessPoolExecutor(max_workers=max_workers) as pex:
            pex.map(
                self.download_url,
                [(url, extract, verification, outdir) for url in self.urls],
            )

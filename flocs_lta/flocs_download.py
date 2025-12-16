#!/usr/bin/env python
import os
from concurrent.futures import ProcessPoolExecutor
from typing import Iterable
import typer
from enum import Enum
from stager_access import get_macaroons, get_webdav_urls_requested
from typer import Argument, Option
from typing_extensions import Annotated, Literal


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
                            os.rename(ms, ms + ".nodysco")
                            os.system(
                                f"DP3 msin={ms+'.nodysco'} msout={ms} msout.storagemanager=dysco steps=[]"
                            )
                            os.remove(outname)
                    except:
                        print(f"{ms} is not a valid MeasurementSet")

    def download_all(
        self, max_workers: int, extract: bool = False, verification: str = "basic"
    ):
        """Download all URLs belonging to the instance.

        Args:
            max_workers (int):
        """
        with ProcessPoolExecutor(max_workers=max_workers) as pex:
            pex.map(
                self.download_url,
                [(url, extract, verification, "") for url in self.urls],
            )


def download(
    stage_id: Annotated[str, Argument(help="StageIt staging ID.")],
    parallel_downloads: Annotated[
        int, Option(help="Maximum number of parallel downloads.")
    ] = 1,
    extract: Annotated[
        bool, Option(help="Extract the tarball after downloading.")
    ] = True,
    verification: Annotated[
        str,
        Option(
            help="Only used when `extract` is True. Sets the verification level to perform after extracting the tarball."
        ),
    ] = "basic",
):
    """Download data from the LTA that was staged via the StageIt service."""
    urls = get_webdav_urls_requested(stage_id)
    macaroons = get_macaroons(stage_id)
    dl = Downloader(urls, macaroons[0])
    dl.download_all(parallel_downloads, extract=extract, verification=verification)


def main():
    typer.run(download)


if __name__ == "__main__":
    typer.run(download)

#!/usr/bin/env python
import os
from typing import Optional

import cyclopts
import structlog
from cyclopts import Parameter
from stager_access import get_macaroons, get_webdav_urls_requested
from typing import Iterable
from typing_extensions import Annotated

from .lta_download import Downloader

app = cyclopts.App()

logger = structlog.getLogger()


@app.command
def download(
    stage_id: Annotated[str, Parameter(help="StageIt staging ID.")],
    parallel_downloads: Annotated[
        Optional[int], Parameter(help="Maximum number of parallel downloads.")
    ] = 1,
    extract: Annotated[
        Optional[bool], Parameter(help="Extract the tarball after downloading.")
    ] = True,
    verification: Annotated[
        Optional[str],
        Parameter(
            help="Only used when `extract` is True. Sets the verification level to perform after extracting the tarball."
        ),
    ] = "basic",
    outdir: Annotated[
        Optional[str],
        Parameter(help="Directory to store downloaded dataproducts in."),
    ] = os.getcwd(),
):
    """Download data from the LTA that was staged via the StageIt service."""
    urls: Optional[Iterable] = get_webdav_urls_requested(stage_id)
    if not urls:
        logger.info("No URLs to download.")
        exit(0)
    macaroons: Optional[list[dict]] = get_macaroons(stage_id)
    if not macaroons:
        raise RuntimeError("No macaroons obtained.")
    else:
        for macaroon in macaroons:
            dl = Downloader(urls, macaroon)
            dl.download_all(
                parallel_downloads,
                extract=extract,
                verification=verification,
                outdir=outdir,
            )


@app.command
def search():
    pass


def main():
    app()


if __name__ == "__main__":
    main()
# vim: ft=python

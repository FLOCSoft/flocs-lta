#!/usr/bin/env python
import os
from typing import Optional

import cyclopts
import structlog
from cyclopts import Parameter
from stager_access import get_macaroons, get_webdav_urls_requested
from typing import Iterable, Literal
from typing_extensions import Annotated

from .lta_download import Downloader
from .lta_search import ObservationStager

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
def search_id(
    sasid: Annotated[
        str,
        Parameter(help="SAS ID to search for."),
    ],
    project: Annotated[
        str,
        Parameter(help="LTA project to limit searches to."),
    ] = "ALL",
    sapi: Annotated[
        Optional[str],
        Parameter(
            help="Sub-array pointing identifier to use in conjunction with `sasid`."
        ),
    ] = "",
    freq_start: Annotated[
        Optional[float],
        Parameter(help="Lower limit to which frequency subbands to select."),
    ] = False,
    freq_end: Annotated[
        Optional[float],
        Parameter(help="Upper limit to which frequency subbands to select."),
    ] = False,
    band: Annotated[
        Literal["HBA", "LBA"],
        Parameter(help="General observing band to search for."),
    ] = "HBA",
    get_surls: Annotated[
        bool,
        Parameter(help="Dump a text file with SURLs for files to be used for staging."),
    ] = False,
    stage_products: Annotated[
        Literal["none", "calibrator", "target", "both"],
        Parameter(help="Stage the specified products."),
    ] = "none",
    n_calibrators: Annotated[
        int,
        Parameter(help="Number of nearest calibrators to search for."),
    ] = 2,
):
    if stage_products != "none":
        get_surls = True

    stager = ObservationStager(get_surls=get_surls)
    stager.find_observation_by_sasid(
        project,
        sasid,
        sapi,
        freq_start,
        freq_end,
    )
    stager.find_nearest_calibrators(n_calibrators, freq_start, freq_end)
    if (stage_products == "calibrator") or (stage_products == "both"):
        stager.stage_calibrators()
    if (stage_products == "target") or (stage_products == "both"):
        stager.stage_target()


@app.command
def search_position(
    ra: Annotated[
        float,
        Parameter(help="Right ascension in degrees of the coordinate of interest."),
    ],
    dec: Annotated[
        float,
        Parameter(help="Declination in degrees of the coordinate of interest."),
    ],
    max_radius: Annotated[
        float,
        Parameter(help="Maximum distance in degrees an observation is allowed to be."),
    ] = 1.3,
    min_duration: Annotated[
        float,
        Parameter(help="Minimum duration in hours an observation must be."),
    ] = 0.0,
    project: Annotated[
        str,
        Parameter(help="LTA project to limit searches to."),
    ] = "ALL",
    freq_start: Annotated[
        Optional[float],
        Parameter(help="Lower limit to which frequency subbands to select."),
    ] = False,
    freq_end: Annotated[
        Optional[float],
        Parameter(help="Upper limit to which frequency subbands to select."),
    ] = False,
    get_surls: Annotated[
        bool,
        Parameter(help="Dump a text file with SURLs for files to be used for staging."),
    ] = False,
    stage_products: Annotated[
        Literal["none", "calibrator", "target", "both"],
        Parameter(help="Stage the specified products."),
    ] = "none",
    band: Annotated[
        Literal["HBA", "LBA"],
        Parameter(help="General observing band to search for."),
    ] = "HBA",
    n_calibrators: Annotated[
        int,
        Parameter(help="Number of nearest calibrators to search for."),
    ] = 2,
):
    if stage_products != "none":
        get_surls = True

    stager = ObservationStager(get_surls=get_surls)
    stager.find_observation_by_position(
        project,
        ra,
        dec,
        max_radius,
        min_duration,
        freq_start,
        freq_end,
    )
    stager.find_nearest_calibrators(n_calibrators, freq_start, freq_end)
    if (stage_products == "calibrator") or (stage_products == "both"):
        stager.stage_calibrators()
    if (stage_products == "target") or (stage_products == "both"):
        stager.stage_target()


def main():
    app()


if __name__ == "__main__":
    main()

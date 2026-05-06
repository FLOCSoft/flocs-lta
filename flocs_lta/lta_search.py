#!/usr/bin/env python
# pyright: reportOperatorIssue=none,reportAttributeAccessIssue=none
import sys
from datetime import timedelta
from typing import Optional

import astropy.units as u
import structlog
from astropy.coordinates import SkyCoord
from awlofar.database.Context import context
from awlofar.main.aweimports import (
    AveragingPipeline,
    CorrelatedDataProduct,
    FileObject,
    Observation,
    SubArrayPointing,
)
from stager_access import stage

logger = structlog.getLogger()


def print_observation_details(obs, sapi: str = ""):
    print(f"Project: {obs.get_project()}")
    print(f"SAS ID: {obs.observationId}")
    if len(obs.subArrayPointings) == 1:
        print(f"SAPI: {obs.subArrayPointings[0].subArrayPointingIdentifier}")
    elif sapi:
        print(f"SAPI: {sapi}")
    print(f"Start time: {obs.startTime}")
    print(f"End time: {obs.endTime}")
    print(f"Duration: {obs.duration} s")
    print(f"Process identifier: {obs.processIdentifierName}")
    print()


class ObservationStager:
    def __init__(self, get_surls: bool = False):
        self.get_surls = get_surls
        self.sapid = None

    def find_observation_by_position(
        self,
        project: str,
        ra: float,
        dec: float,
        radius: float,
        duration: float,
        minfreq: Optional[float] = None,
        maxfreq: Optional[float] = None,
    ):
        context.set_project(project)
        if context.get_current_project().name != project:
            raise ValueError(f"No permissions for project {project}")
        if project == "ALL":
            query = SubArrayPointing.select_all()
        else:
            query = SubArrayPointing.select_all().project_only(project)
        query = (
            query
            # This eliminates the tile beam
            & (SubArrayPointing.numberOfCorrelatedDataProducts > 1)
            & (
                (SubArrayPointing.pointing.rightAscension > ra - 5)
                & (SubArrayPointing.pointing.rightAscension < ra + 5)
            )
            & (
                (SubArrayPointing.pointing.declination > dec - 5)
                & (SubArrayPointing.pointing.declination < dec + 5)
            )
        )
        logger.info(f"Found {len(query)} potential SubArrayPointings.")
        target = None
        if True:
            num_observations = 0
            uris = set()
            pos_target = SkyCoord(ra, dec, unit="deg")

            for obs in query:
                pos_pointing = SkyCoord(
                    obs.pointing.rightAscension, obs.pointing.declination, unit="deg"
                )
                if pos_pointing.separation(pos_target).to("deg") < radius * u.deg:
                    target_obs = obs
                    observations = Observation.subArrayPointings.contains(target_obs)
                    observations &= Observation.isValid == 1
                    observations &= Observation.nrStationsCore > 0
                    observations &= Observation.nrStationsRemote > 0
                    observations &= Observation.nrStationsInternational > 8
                    observations &= Observation.duration > 3600 * duration
                    observations &= (Observation.antennaSet == "HBA Dual Inner") | (
                        Observation.antennaSet == "HBA Dual"
                    )
                    observations = list(observations)
                    if observations:
                        print("== Target observation found ==")
                        target = observations[0]
                        print(f"Project: {target.get_project()}")
                        print(f"Obsid: {target.observationId}")
                        print(f"Duration: {target.duration} s")
                        print(f"Start time: {target.startTime}")
                        print(f"SAPI: {target_obs.subArrayPointingIdentifier}")
                        print(
                            "Distance: ", pos_pointing.separation(pos_target).to("deg")
                        )

                        dataproducts = (
                            CorrelatedDataProduct.subArrayPointing.subArrayPointingIdentifier
                            == target_obs.subArrayPointingIdentifier
                        )
                        dataproducts &= CorrelatedDataProduct.isValid == 1
                        if minfreq:
                            dataproducts &= (
                                CorrelatedDataProduct.minimumFrequency >= minfreq
                            )
                        if maxfreq:
                            dataproducts &= (
                                CorrelatedDataProduct.maximumFrequency <= maxfreq
                            )
                        logger.info(f"Found {len(dataproducts)} CorrelatedDataProducts")
                        if len(dataproducts):
                            num_observations += 1
                        if self.get_surls:
                            if num_observations < 2:
                                for dp in dataproducts:
                                    fo = (
                                        (FileObject.data_object == dp)
                                        & (FileObject.isValid > 0)
                                    ).max("creation_date")
                                    if fo is not None:
                                        uris.add(fo.URI)
                                if not uris:
                                    logger.critical(
                                        "No stageable data matching filter criteria found."
                                    )
                                else:
                                    with open(
                                        f"srms_{target.observationId}.txt", "w"
                                    ) as f:
                                        for uri in sorted(uris):
                                            f.write(uri + "\n")
                else:
                    continue

            if num_observations == 0:
                logger.critical(
                    "No observations containing the target within specified parameters found."
                )
            elif num_observations == 1:
                self.obsid = target.observationId
                self.project = target.get_project()
                self.target = target
                self.target_uris = uris
            else:
                logger.warning(
                    "Multiple observations found, please manually stage preferred one."
                )
                sys.exit(0)

    def find_observation_by_sasid(
        self,
        project: str,
        obsid: str,
        sapid: Optional[str] = None,
        minfreq: Optional[float] = None,
        maxfreq: Optional[float] = None,
    ):
        self.sapid = sapid
        context.set_project(project)
        if context.get_current_project().name != project:
            raise ValueError(f"No permissions for project {project}")
        if project == "ALL":
            query = Observation.select_all()
        else:
            query = Observation.select_all().project_only(project)

        query &= Observation.isValid == 1
        query &= Observation.observationId == obsid
        if not len(query):
            logger.warning("No Observation found, trying AveragingPipeline")
            if project == "ALL":
                query = AveragingPipeline.select_all()
            else:
                query = AveragingPipeline.select_all().project_only(project)
            query &= AveragingPipeline.isValid == 1
            query &= AveragingPipeline.observationId == obsid
            if not len(query):
                logger.critical("No AveragingPipeline products found.")
                sys.exit(0)
        observations = list(query)
        if observations:
            logger.info(f"== {len(observations)} target observation(s) found ==")
            self.target = observations[0]
            sapid = ""
            if type(self.target) is AveragingPipeline:
                sapid = self.target.sourceData[0].subArrayPointingIdentifier
                self.target = self.target.sourceData[0].observations[0]
            self.obsid = self.target.observationId
            self.project = self.target.get_project()
            print_observation_details(self.target, sapi=sapid)

            uris = set()
            self.obsid = self.target.observationId
            self.project = self.target.get_project()

            if self.get_surls:
                logger.info("Obtaining SURLs for dataproducts")
                dataproducts = CorrelatedDataProduct.isValid == 1
                if sapid:
                    dataproducts &= (
                        CorrelatedDataProduct.subArrayPointing.subArrayPointingIdentifier
                        == sapid
                    )
                # SubArrayPointing identifier will already uniquely identify the beam.
                if self.obsid and not sapid:
                    dataproducts = (
                        CorrelatedDataProduct.observation.observationId == self.obsid
                    )
                if minfreq:
                    dataproducts &= CorrelatedDataProduct.minimumFrequency >= minfreq
                if maxfreq:
                    dataproducts &= CorrelatedDataProduct.maximumFrequency <= maxfreq
                for dp in dataproducts:
                    fo = (
                        (FileObject.data_object == dp) & (FileObject.isValid > 0)
                    ).max("creation_date")
                    if fo is not None:
                        uris.add(fo.URI)
                logger.info(f"Found {len(uris)} CorrelatedDataProducts")
                self.target_uris = uris
                if not self.target_uris:
                    logger.critical("No valid URIs found for dataproducts.")
                    sys.exit(0)
                with open(f"srms_{self.target.observationId}.txt", "w") as f:
                    for uri in sorted(uris):
                        print(uri)
                        f.write(uri + "\n")

    def stage_calibrators(self) -> int:
        logger.info("Staging calibrator data")
        id = stage(list(self.calibrator_uris))
        logger.info(f"Staging request submitted with staging ID {id}")
        return id

    def stage_target(self) -> int:
        logger.info("Staging target data")
        id = stage(list(self.target_uris))
        logger.info(f"Staging request submitted with staging ID {id}")
        return id

    def find_nearest_calibrators(
        self,
        n_calibrators: int = 2,
        minfreq: Optional[float] = None,
        maxfreq: Optional[float] = None,
    ):
        logger.info("Searching for nearest calibrators.")
        dt_obs = timedelta(seconds=self.target.duration)
        dt = timedelta(hours=168)

        obs_queries = Observation.select_all().project_only(self.project)
        obs_queries &= (Observation.startTime > self.target.startTime - dt) & (
            Observation.startTime < self.target.startTime + dt_obs + dt
        )
        # HBA calibrator scans are always ~10-15 mins; be a bit lenient.
        obs_queries &= Observation.duration < 3600
        obs_queries &= Observation.observationId != self.target.observationId

        logger.info(f"Identified {len(obs_queries)} potential calibrators.")
        calibrators = list(obs_queries)
        closest_calibrators = sorted(
            calibrators, key=lambda cal: abs(cal.startTime - self.target.startTime)
        )[:n_calibrators]
        for i, cal in enumerate(closest_calibrators, start=1):
            logger.info(f"== Closest calibrator #{i} ==")
            print_observation_details(cal)
        if self.get_surls:
            for cal in closest_calibrators:
                saps = (
                    CorrelatedDataProduct.subArrayPointing.subArrayPointingIdentifier
                    == list(cal.subArrayPointings)[0].subArrayPointingIdentifier
                )
                saps &= CorrelatedDataProduct.isValid == 1
                if minfreq:
                    saps &= CorrelatedDataProduct.minimumFrequency >= minfreq
                if maxfreq:
                    saps &= CorrelatedDataProduct.maximumFrequency <= maxfreq
                uris = set()
                for dp in saps:
                    fo = (
                        (FileObject.data_object == dp) & (FileObject.isValid > 0)
                    ).max("creation_date")
                    if fo is not None:
                        uris.add(fo.URI)
                self.calibrator_uris = uris
                with open(
                    f"srms_{self.target.observationId}_calibrators.txt", "w"
                ) as f:
                    for uri in sorted(uris):
                        f.write(uri + "\n")

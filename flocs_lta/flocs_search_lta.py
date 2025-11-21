#!/usr/bin/env python
import argparse
from datetime import timedelta

import astropy.units as u
import numpy as np
from astropy.coordinates import SkyCoord
from awlofar.database.Context import context
from awlofar.main.aweimports import (
    CorrelatedDataProduct,
    FileObject,
    Observation,
    SubArrayPointing,
)
from stager_access import stage


def print_observation_details(obs):
    print(f"Project: {obs.get_project()}")
    print(f"SAS ID: {obs.observationId}")
    print(f"Start time: {obs.startTime}")
    print(f"End time: {obs.endTime}")
    print(f"Duration: {obs.duration} s")
    print()


class ObservationStager:
    def find_observation_by_position(
        self, project: str, ra: float, dec: float, radius: float, duration: float
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
        print(f"Found {len(query)} potential SubArrayPointings.")
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
                        print(f"== {len(observations)} target observation(s) found ==")
                        target = observations[0]
                        print(f"Project: {target.get_project()}")
                        print(f"Obsid: {target.observationId}")
                        print(f"Duration: {target.duration} s")
                        print(f"Start time: {target.startTime}")
                        print(
                            "Distance: ", pos_pointing.separation(pos_target).to("deg")
                        )

                        dataproducts = (
                            CorrelatedDataProduct.subArrayPointing.subArrayPointingIdentifier
                            == target_obs.subArrayPointingIdentifier
                        )
                        # breakpoint()
                        dataproducts &= CorrelatedDataProduct.isValid == 1
                        dataproducts &= CorrelatedDataProduct.maximumFrequency < 168
                        print(f"Found {len(dataproducts)} CorrelatedDataProducts")
                        num_observations += 1
                        if num_observations < 2:
                            for dp in dataproducts:
                                fo = (
                                    (FileObject.data_object == dp)
                                    & (FileObject.isValid > 0)
                                ).max("creation_date")
                                if fo is not None:
                                    uris.add(fo.URI)
                else:
                    continue

            if not uris:
                print("No stageable data matching filter criteria found.")
            else:
                if num_observations == 1:
                    self.obsid = target.observationId
                    self.project = target.get_project()
                    self.target = target
                    self.target_uris = uris
                    with open(f"srms_{target.observationId}.txt", "w") as f:
                        for uri in sorted(uris):
                            f.write(uri + "\n")
                else:
                    print(
                        "Multiple observations found, please manually stage preferred one."
                    )

    def find_observation_by_sasid(self, project: str, obsid: str):
        context.set_project(project)
        if context.get_current_project().name != project:
            raise ValueError(f"No permissions for project {project}")
        if project == "ALL":
            query = Observation.select_all()
        else:
            query = Observation.select_all().project_only(project)

        query &= Observation.isValid == 1
        query &= Observation.observationId == obsid
        observations = list(query)
        if observations:
            print(f"== {len(observations)} target observation(s) found ==")
            self.target = observations[0]
            self.obsid = self.target.observationId
            self.project = self.target.get_project()
            print_observation_details(self.target)

            uris = set()
            self.obsid = self.target.observationId
            self.project = self.target.get_project()

            dataproducts = (
                CorrelatedDataProduct.subArrayPointing.subArrayPointingIdentifier
                == self.target.subArrayPointings[0].subArrayPointingIdentifier
            )

            for dp in dataproducts:
                fo = ((FileObject.data_object == dp) & (FileObject.isValid > 0)).max(
                    "creation_date"
                )
                if fo is not None:
                    uris.add(fo.URI)
            self.target_uris = uris
            with open(f"srms_{self.target.observationId}.txt", "w") as f:
                for uri in sorted(uris):
                    f.write(uri + "\n")

    def stage_calibrators(self):
        print("Staging calibrator data")
        id = stage(list(self.calibrator_uris))
        print(f"Staging request submitted with staging ID {id}")

    def stage_target(self):
        print("Staging target data")
        id = stage(list(self.target_uris))
        print(f"Staging request submitted with staging ID {id}")

    def find_nearest_calibrators(self):
        print("Searching for nearest calibrators.")
        dt_obs = timedelta(hours=self.target.duration)
        dt = timedelta(hours=1)

        obs_queries = Observation.select_all().project_only(self.project)
        obs_queries &= (Observation.startTime > self.target.startTime - dt) & (
            Observation.startTime < self.target.startTime + dt_obs + dt
        )
        print(f"Identified {len(obs_queries)} potential calibrators.")
        calibrators = list(obs_queries)
        closest = calibrators[0]
        second_closest = calibrators[0]
        # HBA calibrator scans are always ~10-15 mins; be a bit lenient.
        obs_queries &= Observation.duration < 1800
        for cal in obs_queries:
            if np.abs(cal.startTime - self.target.startTime) < np.abs(
                closest.startTime - self.target.startTime
            ):
                second_closest = closest
                closest = cal
            elif (
                np.abs(cal.startTime - self.target.startTime)
                < np.abs(second_closest.startTime - self.target.startTime)
            ) and (cal.observationId != closest.observationId):
                second_closest = cal
        print("== Closest calibrator observation ==")
        print_observation_details(closest)
        if second_closest:
            print("== 2nd closest calibrator observation ==")
            print_observation_details(second_closest)

        saps = (
            CorrelatedDataProduct.subArrayPointing.subArrayPointingIdentifier
            == list(closest.subArrayPointings)[0].subArrayPointingIdentifier
        )
        saps &= CorrelatedDataProduct.isValid == 1
        uris = set()
        for dp in saps:
            fo = ((FileObject.data_object == dp) & (FileObject.isValid > 0)).max(
                "creation_date"
            )
            if fo is not None:
                uris.add(fo.URI)

        if second_closest:
            saps = (
                CorrelatedDataProduct.subArrayPointing.subArrayPointingIdentifier
                == list(second_closest.subArrayPointings)[0].subArrayPointingIdentifier
            )
            saps &= CorrelatedDataProduct.isValid == 1
            for dp in saps:
                fo = ((FileObject.data_object == dp) & (FileObject.isValid > 0)).max(
                    "creation_date"
                )
                if fo is not None:
                    uris.add(fo.URI)
        self.calibrator_uris = uris
        with open(f"srms_{self.target.observationId}_calibrators.txt", "w") as f:
            for uri in sorted(uris):
                f.write(uri + "\n")


def setup_argparser(parser):
    parser.add_argument("--project", help="Project the observation belongs to.")
    parser.add_argument("--obsid", help="ID of the observation without the 'L' prefix.")
    parser.add_argument(
        "--freq_start", help="Search only for subbands at or above this frequency."
    )
    parser.add_argument(
        "--freq_end", help="Search only for subbands at or below this frequency."
    )
    parser.add_argument(
        "--type",
        default="AveragingPipeline",
        choices=["AveragingPipeline"],
        help="Type of data product.",
    )
    parser.add_argument("--ra", type=float, help="Search for this right ascension.")
    parser.add_argument("--dec", type=float, help="Search for this declination")
    parser.add_argument(
        "--max-radius", type=float, help="Maximum allowed separation from target."
    )
    parser.add_argument(
        "--min-duration", type=float, help="Minimum duration of the observation."
    )
    parser.add_argument(
        "--stage",
        action="store_true",
        help="Stage the data after finding it.",
    )


def main():
    parser = argparse.ArgumentParser(
        description="Find a target observation in the LTA and its closest calibrator scans."
    )
    setup_argparser(parser)
    args = parser.parse_args()

    if args.obsid:
        # find_observation(args.project, args.obsid)
        stager = ObservationStager()
        stager.find_observation_by_sasid(args.project, args.obsid)
        # stager.find_nearest_calibrators()
        if args.stage:
            # stager.stage_calibrators()
            stager.stage_target()
    else:
        stager = ObservationStager()
        stager.find_observation_by_position(
            args.project, args.ra, args.dec, args.max_radius, args.min_duration
        )
        stager.find_nearest_calibrators()
        if args.stage:
            stager.stage_calibrators()
            stager.stage_target()


if __name__ == "__main__":
    main()

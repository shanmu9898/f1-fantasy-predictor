import pandas as pd
import numpy as np


class DataCreator:

    @classmethod
    def clean_data(cls):
        raw_data = cls._read_data(folder="archive")
        drivers_clean = cls._clean_drivers_data(raw_data)
        qualifying_clean = cls._clean_qualifying_data(raw_data)


    @classmethod
    def _clean_drivers_data(cls, raw_data):
        driver_ids_list = [1, 4, 853, 854, 822, 830, 815, 847, 849, 846, 817, 839, 840, 20, 852, 842, 841, 8, 832, 844]
        drivers_clean = raw_data.drivers[raw_data.drivers.driverId.isin(driver_ids_list)].drop(
            ["number", "forename", "surname", "dob", "nationality", "url", "driverRef"], axis=1).reset_index(drop=True)
        return drivers_clean

    @classmethod
    def _clean_qualifying_data(cls, raw_data):
        qualifying = raw_data.qualifying.drop(["number", "qualifyId", "position"], axis=1)
        qualifying_base = qualifying[["raceId", "driverId", "constructorId"]].copy()
        qualifying_temp_for_q1 = qualifying.drop(["q2", "q3"], axis=1)
        qualifying_temp_for_q1 = qualifying_temp_for_q1.dropna()
        qualifying_temp_for_q1["q1_position"] = qualifying_temp_for_q1.sort_values(["q1"]).groupby(
            "raceId").cumcount() + 1
        qualifying_temp_for_q2 = qualifying.drop(["q1", "q3"], axis=1)
        qualifying_temp_for_q2 = qualifying_temp_for_q2.dropna()
        qualifying_temp_for_q2["q2_position"] = qualifying_temp_for_q2.sort_values(["q2"]).groupby(
            "raceId").cumcount() + 1
        qualifying_temp_for_q3 = qualifying.drop(["q1", "q2"], axis=1)
        qualifying_temp_for_q3 = qualifying_temp_for_q3.dropna()
        qualifying_temp_for_q3["q3_position"] = qualifying_temp_for_q3.sort_values(["q3"]).groupby(
            "raceId").cumcount() + 1
        qualifying = qualifying_base.merge(
            qualifying_temp_for_q1,
            on=["raceId", "driverId", "constructorId"],
            how="left"
        )
        qualifying = qualifying.merge(
            qualifying_temp_for_q2,
            on=["raceId", "driverId", "constructorId"],
            how="left"
        )
        qualifying = qualifying.merge(
            qualifying_temp_for_q3,
            on=["raceId", "driverId", "constructorId"],
            how="left"
        )
        return qualifying

    @classmethod
    def _read_data(cls, folder: str):
        drivers = pd.read_csv(f"{folder}/drivers.csv")
        qualifying = pd.read_csv(f"{folder}/qualifying.csv")
        constructor_results = pd.read_csv(f"{folder}/constructor_results.csv")
        race_results = pd.read_csv(f"{folder}/results.csv")
        constructors = pd.read_csv(f"{folder}/constructors.csv")
        return RawData(
            drivers=drivers,
            qualifying=qualifying,
            constructor_results=constructor_results,
            race_results=race_results,
            constructors=constructors
        )


class RawData:

    def __init__(self, drivers, qualifying, constructor_results, race_results, constructors):
        self.drivers = drivers
        self.qualifying = qualifying
        self.constructor_results = constructor_results
        self.race_results = race_results
        self.constructor_results = constructors

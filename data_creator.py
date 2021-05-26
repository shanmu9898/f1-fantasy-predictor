import pandas as pd
import numpy as np


class DataCreator:

    @classmethod
    def clean_data(cls):
        raw_data = cls._read_data(folder="archive")
        drivers_clean = cls._clean_drivers_data(raw_data)
        qualifying_clean = cls._clean_qualifying_data(raw_data)
        final_data = cls._generate_final_data(qualifying_clean, drivers_clean, raw_data)


    @classmethod
    def _clean_drivers_data(cls, raw_data):
        driver_ids_list = [1, 4, 853, 854, 822, 830, 815, 847, 849, 846, 817, 839, 840, 20, 852, 842, 841, 8, 832, 844]
        drivers_clean = raw_data.drivers[raw_data.drivers.driverId.isin(driver_ids_list)].drop(
            ["number", "forename", "surname", "dob", "nationality", "url", "driverRef"], axis=1).reset_index(drop=True)
        return drivers_clean

    @classmethod
    def _copy_quali_data(cls, df):
        if df["q2_position"].isna().values.all():
            df["q2_position"] = df["q1_position"]
        if df["q3_position"].isna().values.all():
            df["q3_position"] = df["q2_position"]
        return df

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

        qualifying = qualifying.drop(
            ["q1", "q2", "q3"],
            axis=1
        )

        qualifying_clean = qualifying.groupby(["raceId"]).transform(lambda x: x.fillna(x.max() + 1))
        qualifying_clean["raceId"] = qualifying["raceId"]

        qualifying_final = qualifying_clean.groupby(["raceId"]).apply(cls._copy_quali_data)

        return qualifying_final

    @classmethod
    def _read_data(cls, folder: str):
        drivers = pd.read_csv(f"{folder}/drivers.csv")
        qualifying = pd.read_csv(f"{folder}/qualifying.csv")
        constructor_standings = pd.read_csv(f"{folder}/constructor_standings.csv")
        race_results = pd.read_csv(f"{folder}/results.csv")
        races = pd.read_csv(f"{folder}/races.csv")
        return RawData(
            drivers=drivers,
            qualifying=qualifying,
            constructor_standings=constructor_standings,
            race_results=race_results,
            races=races
        )

    @classmethod
    def _generate_final_data(cls, qualifying_clean, drivers_clean, raw_data):
        quali_race_results = qualifying_clean.merge(
            raw_data.race_results,
            on=["raceId", "driverId", "constructorId"],
            how="left"
        )

        quali_race_results =  quali_race_results.drop(
            ["resultId", "number", "grid", "positionText",
             "positionOrder", "points","laps", "time",
             "milliseconds", "fastestLap", "rank",
             "fastestLapTime", "fastestLapSpeed", "statusId"],
            axis=1
        )

        quali_race_results_clean = quali_race_results.copy()
        quali_race_results_clean = quali_race_results_clean.groupby(["raceId"]).transform(lambda x: x.fillna(x.max() + 1))
        quali_race_results_clean["raceId"] = quali_race_results["raceId"]

        quali_race_results_clean = quali_race_results_clean.rename(columns=
            {
                "position": "driver_standing"
            }
        )

        # merging-in constructor results
        results = quali_race_results_clean.copy()
        results_w_races = results.merge(
            raw_data.races,
            on="raceId",
            how="left"
        )

        results_w_races = results_w_races.drop(
            ["round", "name", "date", "time", "url"],
            axis=1
        )

        constructors_clean = raw_data.constructor_standings.copy()
        constructors_clean = constructors_clean.drop(
            ["constructorStandingssId", "position", "positionText", "wins"],
            axis=1
        )

        constructors_clean = constructors_clean.rename(
            columns={
                "points": "constructor_standing"
            }
        )

        final_data = results_w_races.merge(
            constructors_clean,
            on=["raceId", "constructorId"],
            how="left"
        )

        final_data = final_data.merge(
            drivers_clean,
            on="driverId",
            how="right"
        )

        final_data.columns = ["driver_id", "constructor_id",
                              "q1_position", "q2_position",
                              "q3_position", "driver_standing", "race_id",
                              "year", "circuit_id", "constructor_standing",
                              "driver_code"]

        return final_data

class RawData:

    def __init__(self, drivers, qualifying, constructor_standings, race_results, races):
        self.drivers = drivers
        self.qualifying = qualifying
        self.constructor_standings = constructor_standings
        self.race_results = race_results
        self.races = races

import pandas as pd


def process_fuel_data(
    df: pd.DataFrame,
    pct_refuel_threshold: float = 5.0,
    litres_refuel_threshold: float = 10.0,
    noise_threshold: float = 2.0,
    high_confidence_max_gap_min: int = 30,
    medium_confidence_max_gap_min: int = 720,
    large_gap_min: int = 720,
    staircase_max_gap_min: int = 10,
    staircase_max_session_min: int = 20,
) -> pd.DataFrame:
    """
    Process raw fuel candidate rows and return final analyzed rows.
    """

    if df.empty:
        return df.copy()

    result = df.copy()

    # 1) Convert datetime columns
    result["DATAEVENTO"] = pd.to_datetime(result["DATAEVENTO"], errors="coerce")
    result["SNAP_DATAPOSIZIONE"] = pd.to_datetime(result["SNAP_DATAPOSIZIONE"], errors="coerce")

    # 2) Sort rows
    result = result.sort_values(
        by=["ID_MEZZO", "DATAEVENTO", "ID_EVENT", "PriorityGroup"],
        ascending=[True, True, True, True]
    ).reset_index(drop=True)

    # 3) Keep best row per same mezzo + timestamp
    result = (
        result.sort_values(
            by=["ID_MEZZO", "DATAEVENTO", "PriorityGroup", "ID_EVENT"],
            ascending=[True, True, True, True]
        )
        .drop_duplicates(subset=["ID_MEZZO", "DATAEVENTO"], keep="first")
        .reset_index(drop=True)
    )

    # 4) Sort again after dedup
    result = result.sort_values(
        by=["ID_MEZZO", "DATAEVENTO", "ID_EVENT"],
        ascending=[True, True, True]
    ).reset_index(drop=True)

    # 5) Previous row values
    result["PrevFuelValue"] = result.groupby("ID_MEZZO")["FuelValue"].shift(1)
    result["PrevDataEvento"] = result.groupby("ID_MEZZO")["DATAEVENTO"].shift(1)

    # 6) Fuel delta
    result["FuelDelta"] = result["FuelValue"] - result["PrevFuelValue"]

    # 7) Gap minutes
    result["GapMinutes"] = (
        (result["DATAEVENTO"] - result["PrevDataEvento"]).dt.total_seconds() / 60
    )

    # 8) Small positive noise
    result["IsSmallPositiveNoise"] = (
        result["FuelDelta"].notna()
        & (result["FuelDelta"] > 0)
        & (result["FuelDelta"] <= noise_threshold)
    ).astype(int)

    # 9) Possible refuel

    MIN_VALID_FUEL = 5

    result["IsSensorReset"] = (
        result["PrevFuelValue"].notna()
        & (result["PrevFuelValue"] < MIN_VALID_FUEL)
        & (result["FuelValue"] > 50)
    ).astype(int)

    invalid_prev_mask = result["PrevFuelValue"] < MIN_VALID_FUEL

    result["IsPossibleRefuel"] = 0

    pct_mask = (
        (result["IsPercentage"] == 1)
        & result["FuelDelta"].notna()
        & (result["FuelDelta"] >= pct_refuel_threshold)
    )

    litres_mask = (
        (result["IsPercentage"] == 0)
        & result["FuelDelta"].notna()
        & (result["FuelDelta"] >= litres_refuel_threshold)
    )

    result.loc[(pct_mask | litres_mask) & ~invalid_prev_mask, "IsPossibleRefuel"] = 1

    # 9b) Staircase/session-based refuel detection
    # Group consecutive short-interval positive increases into one candidate refuel session
    result["IsStaircaseRefuel"] = 0

    def finalize_staircase_session(
        session_rows,
        session_start_fuel,
        session_is_percentage,
    ):
        if len(session_rows) < 2:
            return

        last_idx = session_rows[-1]
        last_fuel = result.at[last_idx, "FuelValue"]

        if pd.isna(session_start_fuel) or pd.isna(last_fuel):
            return

        total_increase = last_fuel - session_start_fuel

        if session_is_percentage == 1:
            qualifies = total_increase >= pct_refuel_threshold
        else:
            qualifies = total_increase >= litres_refuel_threshold

        if qualifies:
            # keep one refuel signal on the last row of the staircase session
            result.loc[session_rows, "IsPossibleRefuel"] = 0
            result.at[last_idx, "IsPossibleRefuel"] = 1
            result.at[last_idx, "IsStaircaseRefuel"] = 1

    for _, vehicle_df in result.groupby("ID_MEZZO", sort=False):
        vehicle_indices = vehicle_df.index.tolist()

        session_rows = []
        session_start_fuel = None
        session_start_time = None
        session_is_percentage = None

        for idx in vehicle_indices:
            fuel_delta = result.at[idx, "FuelDelta"]
            gap_minutes = result.at[idx, "GapMinutes"]
            prev_fuel = result.at[idx, "PrevFuelValue"]
            current_time = result.at[idx, "DATAEVENTO"]
            current_is_percentage = result.at[idx, "IsPercentage"]
            is_sensor_reset = result.at[idx, "IsSensorReset"]

            is_rising_candidate = (
                pd.notna(fuel_delta)
                and fuel_delta > 0
                and pd.notna(gap_minutes)
                and gap_minutes <= staircase_max_gap_min
                and pd.notna(prev_fuel)
                and prev_fuel >= MIN_VALID_FUEL
                and is_sensor_reset == 0
            )

            if not is_rising_candidate:
                finalize_staircase_session(
                    session_rows=session_rows,
                    session_start_fuel=session_start_fuel,
                    session_is_percentage=session_is_percentage,
                )
                session_rows = []
                session_start_fuel = None
                session_start_time = None
                session_is_percentage = None
                continue

            if not session_rows:
                session_rows = [idx]
                session_start_fuel = prev_fuel
                session_start_time = result.at[idx, "PrevDataEvento"]
                session_is_percentage = current_is_percentage
                continue

            session_duration_min = None
            if pd.notna(session_start_time) and pd.notna(current_time):
                session_duration_min = (current_time - session_start_time).total_seconds() / 60

            same_unit_type = current_is_percentage == session_is_percentage
            within_session_window = (
                session_duration_min is not None
                and session_duration_min <= staircase_max_session_min
            )

            if same_unit_type and within_session_window:
                session_rows.append(idx)
            else:
                finalize_staircase_session(
                    session_rows=session_rows,
                    session_start_fuel=session_start_fuel,
                    session_is_percentage=session_is_percentage,
                )
                session_rows = [idx]
                session_start_fuel = prev_fuel
                session_start_time = result.at[idx, "PrevDataEvento"]
                session_is_percentage = current_is_percentage

        finalize_staircase_session(
            session_rows=session_rows,
            session_start_fuel=session_start_fuel,
            session_is_percentage=session_is_percentage,
        )

    # 10) Confidence
    result["RefuelConfidence"] = "NONE"

    high_mask = (
        (result["IsPossibleRefuel"] == 1)
        & result["GapMinutes"].notna()
        & (result["GapMinutes"] <= high_confidence_max_gap_min)
    )

    medium_mask = (
        (result["IsPossibleRefuel"] == 1)
        & result["GapMinutes"].notna()
        & (result["GapMinutes"] > high_confidence_max_gap_min)
        & (result["GapMinutes"] <= medium_confidence_max_gap_min)
    )

    low_mask = (
        (result["IsPossibleRefuel"] == 1)
        & result["GapMinutes"].notna()
        & (result["GapMinutes"] > medium_confidence_max_gap_min)
    )

    result.loc[high_mask, "RefuelConfidence"] = "HIGH"
    result.loc[medium_mask, "RefuelConfidence"] = "MEDIUM"
    result.loc[low_mask, "RefuelConfidence"] = "LOW"

    # 11) Large gap marker
    result["HasLargeGapBefore"] = (
        result["GapMinutes"].notna()
        & (result["GapMinutes"] > large_gap_min)
    ).astype(int)

    # 12) Final column order
    preferred_order = [
        "ID_EVENT",
        "ID_STORICO",
        "DATAEVENTO",
        "PrevDataEvento",
        "SNAP_DATAPOSIZIONE",
        "ID_MEZZO",
        "TARGA",
        "FuelValue",
        "PrevFuelValue",
        "FuelDelta",
        "FuelUnit",
        "IsPercentage",
        "GapMinutes",
        "PriorityGroup",
        "PacketCode",
        "Titolo",
        "Y_Titolo",
        "NOME_CODICE",
        "LAT",
        "LON",
        "IsSmallPositiveNoise",
        "IsSensorReset",
        "IsStaircaseRefuel",
        "IsPossibleRefuel",
        "RefuelConfidence",
        "HasLargeGapBefore",
    ]

    existing_columns = [col for col in preferred_order if col in result.columns]
    remaining_columns = [col for col in result.columns if col not in existing_columns]

    result = result[existing_columns + remaining_columns]

    return result
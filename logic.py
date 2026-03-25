import pandas as pd


def process_fuel_data(
    df: pd.DataFrame,
    pct_refuel_threshold: float = 5.0,
    litres_refuel_threshold: float = 10.0,
    noise_threshold: float = 2.0,
    high_confidence_max_gap_min: int = 30,
    medium_confidence_max_gap_min: int = 720,
    large_gap_min: int = 720,
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
        "IsPossibleRefuel",
        "RefuelConfidence",
        "HasLargeGapBefore",
    ]

    existing_columns = [col for col in preferred_order if col in result.columns]
    remaining_columns = [col for col in result.columns if col not in existing_columns]

    result = result[existing_columns + remaining_columns]

    return result
import pyodbc
import pandas as pd
from config import DB_DRIVER, DEFAULT_DATABASE, DOMAIN_CONFIG


def get_domain_settings(domain: str):
    domain = domain.upper().strip()
    if domain not in DOMAIN_CONFIG:
        raise ValueError(f"Unsupported domain: {domain}")
    return DOMAIN_CONFIG[domain]


def get_connection(server: str, database: str = DEFAULT_DATABASE):
    conn_str = (
        f"DRIVER={{{DB_DRIVER}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


def fetch_fuel_rows(
    domain: str,
    date_from: str,
    date_to: str,
    id_mezzo: int | None = None,
    targa: str | None = None,
) -> pd.DataFrame:
    """
    Fetch candidate fuel rows for one domain.
    Priority rule:
    - if id_mezzo is provided, use ID_MEZZO query
    - else if targa is provided, use TARGA query
    - else raise error
    """

    if id_mezzo is None and (targa is None or not targa.strip()):
        raise ValueError("You must provide either id_mezzo or targa.")

    settings = get_domain_settings(domain)
    server = settings["server"]
    story_db = settings["story_db"]
    core_db = settings["core_db"]

    if id_mezzo is not None:
        query = f"""
        ------------------------------------------------------------
        -- INPUT PARAMETERS
        ------------------------------------------------------------
        DECLARE @ID_MEZZO INT = ?;
        DECLARE @DateFrom DATETIME = ?;
        DECLARE @DateTo   DATETIME = ?;

        DECLARE @DateToNextDay DATETIME =
            CASE 
                WHEN @DateTo IS NULL THEN NULL
                ELSE DATEADD(DAY, 1, @DateTo)
            END;

        ------------------------------------------------------------
        -- 1) KEEP ONLY FUEL-RELATED SIGNAL DEFINITIONS
        ------------------------------------------------------------
        WITH FuelConfig AS (
            SELECT
                cd.ID AS ID_DETTAGLIO,
                cd.PacketCode,
                cd.Titolo,
                cd.Y_Titolo,
                cd.NOME_CODICE,

                CASE 
                    WHEN cd.NOME_CODICE LIKE 'FMS%' THEN 1
                    WHEN cd.NOME_CODICE LIKE 'LVC%' THEN 2
                    WHEN cd.NOME_CODICE LIKE 'OBU%' THEN 3
                    ELSE 4
                END AS PriorityGroup,

                CASE 
                    WHEN cd.PacketCode LIKE '%fuellevelpercent%' THEN '%'
                    WHEN cd.PacketCode LIKE '%fuellevel2percent%' THEN '%'
                    WHEN cd.PacketCode LIKE '%fuellevel%' AND cd.Y_Titolo = '%' THEN '%'
                    WHEN cd.PacketCode LIKE '%lnglevelpercent%' THEN '%'
                    WHEN cd.PacketCode LIKE '%cnglevelpercent%' THEN '%'
                    WHEN cd.PacketCode LIKE '%reeferfuellevelpercent%' THEN '%'
                    WHEN cd.PacketCode LIKE '%adbluelevelpercent%' THEN '%'
                    WHEN cd.Titolo LIKE '%carburante (%)%' THEN '%'
                    WHEN cd.Titolo LIKE '%adblue level (%)%' THEN '%'
                    WHEN cd.PacketCode LIKE '%fuellevel%' AND cd.Y_Titolo = 'L' THEN 'L'
                    WHEN cd.PacketCode LIKE '%reeferfuellevel%' AND cd.Y_Titolo = 'L' THEN 'L'
                    ELSE NULL
                END AS FuelUnit,

                CASE 
                    WHEN cd.PacketCode LIKE '%fuellevelpercent%' THEN 1
                    WHEN cd.PacketCode LIKE '%fuellevel2percent%' THEN 1
                    WHEN cd.PacketCode LIKE '%fuellevel%' AND cd.Y_Titolo = '%' THEN 1
                    WHEN cd.PacketCode LIKE '%lnglevelpercent%' THEN 1
                    WHEN cd.PacketCode LIKE '%cnglevelpercent%' THEN 1
                    WHEN cd.PacketCode LIKE '%reeferfuellevelpercent%' THEN 1
                    WHEN cd.PacketCode LIKE '%adbluelevelpercent%' THEN 1
                    WHEN cd.Titolo LIKE '%carburante (%)%' THEN 1
                    WHEN cd.Titolo LIKE '%adblue level (%)%' THEN 1
                    WHEN cd.PacketCode LIKE '%fuellevel%' AND cd.Y_Titolo = 'L' THEN 0
                    WHEN cd.PacketCode LIKE '%reeferfuellevel%' AND cd.Y_Titolo = 'L' THEN 0
                    ELSE NULL
                END AS IsPercentage
            FROM {core_db}.dbo.CANBUS_CFG_DETTAGLIO cd
            WHERE
                cd.PacketCode LIKE '%fuellevelpercent%'
                OR cd.PacketCode LIKE '%fuellevel2percent%'
                OR (cd.PacketCode LIKE '%fuellevel%' AND cd.Y_Titolo IN ('%', 'L'))
                OR cd.PacketCode LIKE '%lnglevelpercent%'
                OR cd.PacketCode LIKE '%cnglevelpercent%'
                OR cd.PacketCode LIKE '%reeferfuellevelpercent%'
                OR (cd.PacketCode LIKE '%reeferfuellevel%' AND cd.Y_Titolo = 'L')
                OR cd.PacketCode LIKE '%adbluelevelpercent%'
                OR cd.Titolo LIKE '%carburante (%)%'
                OR cd.Titolo LIKE '%adblue level (%)%'
        ),

        ------------------------------------------------------------
        -- 2) KEEP ONLY FUEL STREAMS FOR THE REQUESTED MEZZO
        ------------------------------------------------------------
        FuelStreams AS (
            SELECT
                iem.ID AS ID_INFO_EXT_MEZZO,
                iem.ID_MEZZO,
                iem.ID_DETTAGLIO,
                fc.PacketCode,
                fc.Titolo,
                fc.Y_Titolo,
                fc.NOME_CODICE,
                fc.PriorityGroup,
                fc.FuelUnit,
                fc.IsPercentage,
                ma.TARGA
            FROM {core_db}.dbo.INFO_EXT_MEZZO iem
            INNER JOIN FuelConfig fc
                ON iem.ID_DETTAGLIO = fc.ID_DETTAGLIO
            LEFT JOIN {core_db}.dbo.MEZZO_ANAGRAFICA ma
                ON iem.ID_MEZZO = ma.ID
            WHERE iem.ID_MEZZO = @ID_MEZZO
        ),

        ------------------------------------------------------------
        -- 3) GET ONLY EVENTS FOR THOSE FUEL STREAMS IN DATE RANGE
        ------------------------------------------------------------
        FuelEvents AS (
            SELECT
                ies.ID AS ID_EVENT,
                ies.ID_STORICO,
                ies.ID_INFO_EXT_MEZZO,
                ies.VALFLOAT,
                ies.DATAEVENTO,
                ies.ERRORE
            FROM {story_db}.dbo.INFO_EXT_STO ies
            INNER JOIN FuelStreams fs
                ON ies.ID_INFO_EXT_MEZZO = fs.ID_INFO_EXT_MEZZO
            WHERE (@DateFrom IS NULL OR ies.DATAEVENTO >= @DateFrom)
              AND (@DateToNextDay IS NULL OR ies.DATAEVENTO < @DateToNextDay)
        ),

        ------------------------------------------------------------
        -- 4) FINAL ENRICHMENT
        ------------------------------------------------------------
        FinalData AS (
            SELECT
                fe.ID_EVENT,
                fe.ID_STORICO,
                fe.DATAEVENTO,
                st.DATAPOSIZIONE AS SNAP_DATAPOSIZIONE,
                fs.ID_MEZZO,
                fs.TARGA,
                fe.VALFLOAT AS FuelValue,
                fs.FuelUnit,
                fs.IsPercentage,
                fs.PriorityGroup,
                fs.PacketCode,
                fs.Titolo,
                fs.Y_Titolo,
                fs.NOME_CODICE,
                st.LAT,
                st.LON
            FROM FuelEvents fe
            INNER JOIN FuelStreams fs
                ON fe.ID_INFO_EXT_MEZZO = fs.ID_INFO_EXT_MEZZO
            LEFT JOIN {story_db}.dbo.STORICO st
                ON fe.ID_STORICO = st.ID
        )

        ------------------------------------------------------------
        -- FINAL OUTPUT
        ------------------------------------------------------------
        SELECT
            ID_EVENT,
            ID_STORICO,
            DATAEVENTO,
            SNAP_DATAPOSIZIONE,
            ID_MEZZO,
            TARGA,
            FuelValue,
            FuelUnit,
            IsPercentage,
            PriorityGroup,
            PacketCode,
            Titolo,
            Y_Titolo,
            NOME_CODICE,
            LAT,
            LON
        FROM FinalData
        WHERE FuelValue IS NOT NULL
        ORDER BY ID_MEZZO, DATAEVENTO, ID_EVENT;
        """
        params = [id_mezzo, date_from, date_to]

    else:
        targa = targa.strip()

        query = f"""
        ------------------------------------------------------------
        -- INPUT PARAMETERS
        ------------------------------------------------------------
        DECLARE @TARGA NVARCHAR(100) = ?;
        DECLARE @DateFrom DATETIME = ?;
        DECLARE @DateTo   DATETIME = ?;

        DECLARE @DateToNextDay DATETIME =
            CASE 
                WHEN @DateTo IS NULL THEN NULL
                ELSE DATEADD(DAY, 1, @DateTo)
            END;

        ------------------------------------------------------------
        -- 1) KEEP ONLY FUEL-RELATED SIGNAL DEFINITIONS
        ------------------------------------------------------------
        WITH FuelConfig AS (
            SELECT
                cd.ID AS ID_DETTAGLIO,
                cd.PacketCode,
                cd.Titolo,
                cd.Y_Titolo,
                cd.NOME_CODICE,

                CASE 
                    WHEN cd.NOME_CODICE LIKE 'FMS%' THEN 1
                    WHEN cd.NOME_CODICE LIKE 'LVC%' THEN 2
                    WHEN cd.NOME_CODICE LIKE 'OBU%' THEN 3
                    ELSE 4
                END AS PriorityGroup,

                CASE 
                    WHEN cd.PacketCode LIKE '%fuellevelpercent%' THEN '%'
                    WHEN cd.PacketCode LIKE '%fuellevel2percent%' THEN '%'
                    WHEN cd.PacketCode LIKE '%fuellevel%' AND cd.Y_Titolo = '%' THEN '%'
                    WHEN cd.PacketCode LIKE '%lnglevelpercent%' THEN '%'
                    WHEN cd.PacketCode LIKE '%cnglevelpercent%' THEN '%'
                    WHEN cd.PacketCode LIKE '%reeferfuellevelpercent%' THEN '%'
                    WHEN cd.PacketCode LIKE '%adbluelevelpercent%' THEN '%'
                    WHEN cd.Titolo LIKE '%carburante (%)%' THEN '%'
                    WHEN cd.Titolo LIKE '%adblue level (%)%' THEN '%'
                    WHEN cd.PacketCode LIKE '%fuellevel%' AND cd.Y_Titolo = 'L' THEN 'L'
                    WHEN cd.PacketCode LIKE '%reeferfuellevel%' AND cd.Y_Titolo = 'L' THEN 'L'
                    ELSE NULL
                END AS FuelUnit,

                CASE 
                    WHEN cd.PacketCode LIKE '%fuellevelpercent%' THEN 1
                    WHEN cd.PacketCode LIKE '%fuellevel2percent%' THEN 1
                    WHEN cd.PacketCode LIKE '%fuellevel%' AND cd.Y_Titolo = '%' THEN 1
                    WHEN cd.PacketCode LIKE '%lnglevelpercent%' THEN 1
                    WHEN cd.PacketCode LIKE '%cnglevelpercent%' THEN 1
                    WHEN cd.PacketCode LIKE '%reeferfuellevelpercent%' THEN 1
                    WHEN cd.PacketCode LIKE '%adbluelevelpercent%' THEN 1
                    WHEN cd.Titolo LIKE '%carburante (%)%' THEN 1
                    WHEN cd.Titolo LIKE '%adblue level (%)%' THEN 1
                    WHEN cd.PacketCode LIKE '%fuellevel%' AND cd.Y_Titolo = 'L' THEN 0
                    WHEN cd.PacketCode LIKE '%reeferfuellevel%' AND cd.Y_Titolo = 'L' THEN 0
                    ELSE NULL
                END AS IsPercentage
            FROM {core_db}.dbo.CANBUS_CFG_DETTAGLIO cd
            WHERE
                cd.PacketCode LIKE '%fuellevelpercent%'
                OR cd.PacketCode LIKE '%fuellevel2percent%'
                OR (cd.PacketCode LIKE '%fuellevel%' AND cd.Y_Titolo IN ('%', 'L'))
                OR cd.PacketCode LIKE '%lnglevelpercent%'
                OR cd.PacketCode LIKE '%cnglevelpercent%'
                OR cd.PacketCode LIKE '%reeferfuellevelpercent%'
                OR (cd.PacketCode LIKE '%reeferfuellevel%' AND cd.Y_Titolo = 'L')
                OR cd.PacketCode LIKE '%adbluelevelpercent%'
                OR cd.Titolo LIKE '%carburante (%)%'
                OR cd.Titolo LIKE '%adblue level (%)%'
        ),

        ------------------------------------------------------------
        -- 2) KEEP ONLY FUEL STREAMS FOR THE REQUESTED TARGA
        ------------------------------------------------------------
        FuelStreams AS (
            SELECT
                iem.ID AS ID_INFO_EXT_MEZZO,
                iem.ID_MEZZO,
                iem.ID_DETTAGLIO,
                fc.PacketCode,
                fc.Titolo,
                fc.Y_Titolo,
                fc.NOME_CODICE,
                fc.PriorityGroup,
                fc.FuelUnit,
                fc.IsPercentage,
                ma.TARGA
            FROM {core_db}.dbo.INFO_EXT_MEZZO iem
            INNER JOIN FuelConfig fc
                ON iem.ID_DETTAGLIO = fc.ID_DETTAGLIO
            INNER JOIN {core_db}.dbo.MEZZO_ANAGRAFICA ma
                ON iem.ID_MEZZO = ma.ID
            WHERE LTRIM(RTRIM(ma.TARGA)) = LTRIM(RTRIM(@TARGA))
        ),

        ------------------------------------------------------------
        -- 3) GET ONLY EVENTS FOR THOSE FUEL STREAMS IN DATE RANGE
        ------------------------------------------------------------
        FuelEvents AS (
            SELECT
                ies.ID AS ID_EVENT,
                ies.ID_STORICO,
                ies.ID_INFO_EXT_MEZZO,
                ies.VALFLOAT,
                ies.DATAEVENTO,
                ies.ERRORE
            FROM {story_db}.dbo.INFO_EXT_STO ies
            INNER JOIN FuelStreams fs
                ON ies.ID_INFO_EXT_MEZZO = fs.ID_INFO_EXT_MEZZO
            WHERE (@DateFrom IS NULL OR ies.DATAEVENTO >= @DateFrom)
              AND (@DateToNextDay IS NULL OR ies.DATAEVENTO < @DateToNextDay)
        ),

        ------------------------------------------------------------
        -- 4) FINAL ENRICHMENT
        ------------------------------------------------------------
        FinalData AS (
            SELECT
                fe.ID_EVENT,
                fe.ID_STORICO,
                fe.DATAEVENTO,
                st.DATAPOSIZIONE AS SNAP_DATAPOSIZIONE,
                fs.ID_MEZZO,
                fs.TARGA,
                fe.VALFLOAT AS FuelValue,
                fs.FuelUnit,
                fs.IsPercentage,
                fs.PriorityGroup,
                fs.PacketCode,
                fs.Titolo,
                fs.Y_Titolo,
                fs.NOME_CODICE,
                st.LAT,
                st.LON
            FROM FuelEvents fe
            INNER JOIN FuelStreams fs
                ON fe.ID_INFO_EXT_MEZZO = fs.ID_INFO_EXT_MEZZO
            LEFT JOIN {story_db}.dbo.STORICO st
                ON fe.ID_STORICO = st.ID
        )

        ------------------------------------------------------------
        -- FINAL OUTPUT
        ------------------------------------------------------------
        SELECT
            ID_EVENT,
            ID_STORICO,
            DATAEVENTO,
            SNAP_DATAPOSIZIONE,
            ID_MEZZO,
            TARGA,
            FuelValue,
            FuelUnit,
            IsPercentage,
            PriorityGroup,
            PacketCode,
            Titolo,
            Y_Titolo,
            NOME_CODICE,
            LAT,
            LON
        FROM FinalData
        WHERE FuelValue IS NOT NULL
        ORDER BY ID_MEZZO, DATAEVENTO, ID_EVENT;
        """
        params = [targa, date_from, date_to]

    conn = get_connection(server, "master")
    try:
        df = pd.read_sql(query, conn, params=params)
    finally:
        conn.close()

    return df
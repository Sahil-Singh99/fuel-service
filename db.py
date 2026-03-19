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
    Fetch candidate fuel rows for one domain, filtered by either ID_MEZZO or TARGA.
    At least one of id_mezzo or targa must be provided.
    """

    if id_mezzo is None and (targa is None or not targa.strip()):
        raise ValueError("You must provide either id_mezzo or targa.")

    settings = get_domain_settings(domain)

    server = settings["server"]
    story_db = settings["story_db"]
    core_db = settings["core_db"]

    query = f"""
    DECLARE @ID_MEZZO INT = ?;
    DECLARE @TARGA NVARCHAR(100) = ?;
    DECLARE @DateFrom DATETIME = ?;
    DECLARE @DateTo   DATETIME = ?;

    DECLARE @DateToNextDay DATETIME =
        CASE 
            WHEN @DateTo IS NULL THEN NULL
            ELSE DATEADD(DAY, 1, @DateTo)
        END;

    WITH BaseEvents AS (
        SELECT 
            ies.ID AS ID_EVENT,
            ies.ID_STORICO,
            ies.ID_INFO_EXT_MEZZO,
            ies.VALFLOAT,
            ies.DATAEVENTO,
            ies.ERRORE
        FROM {story_db}.dbo.INFO_EXT_STO ies
        WHERE (@DateFrom IS NULL OR ies.DATAEVENTO >= @DateFrom)
          AND (@DateToNextDay IS NULL OR ies.DATAEVENTO < @DateToNextDay)
    ),

    Enriched AS (
        SELECT
            B.*,
            st.LAT,
            st.LON,
            st.DATAPOSIZIONE AS SNAP_DATAPOSIZIONE,

            iem.ID_MEZZO,
            iem.ID_DETTAGLIO,

            cd.PacketCode,
            cd.Titolo,
            cd.Y_Titolo,
            cd.NOME_CODICE,

            MA.TARGA

        FROM BaseEvents B
        LEFT JOIN {story_db}.dbo.STORICO st 
               ON B.ID_STORICO = st.ID
        LEFT JOIN {core_db}.dbo.INFO_EXT_MEZZO iem
               ON B.ID_INFO_EXT_MEZZO = iem.ID
        LEFT JOIN {core_db}.dbo.CANBUS_CFG_DETTAGLIO cd
               ON iem.ID_DETTAGLIO = cd.ID
        LEFT JOIN {core_db}.dbo.MEZZO_ANAGRAFICA MA
               ON iem.ID_MEZZO = MA.ID
        WHERE
            (@ID_MEZZO IS NULL OR iem.ID_MEZZO = @ID_MEZZO)
            AND (@TARGA IS NULL OR LTRIM(RTRIM(MA.TARGA)) = LTRIM(RTRIM(@TARGA)))
    ),

    FuelDetection AS (
        SELECT 
            E.*,

            CASE 
                WHEN E.NOME_CODICE LIKE 'FMS%' THEN 1
                WHEN E.NOME_CODICE LIKE 'LVC%' THEN 2
                WHEN E.NOME_CODICE LIKE 'OBU%' THEN 3
                ELSE 4
            END AS PriorityGroup,

            CASE 
                WHEN LOWER(PacketCode) LIKE '%fuellevelpercent%' THEN VALFLOAT
                WHEN LOWER(PacketCode) LIKE '%fuellevel2percent%' THEN VALFLOAT
                WHEN LOWER(PacketCode) LIKE '%fuellevel%' AND Y_Titolo = '%' THEN VALFLOAT
                WHEN LOWER(PacketCode) LIKE '%lnglevelpercent%' THEN VALFLOAT
                WHEN LOWER(PacketCode) LIKE '%cnglevelpercent%' THEN VALFLOAT
                WHEN LOWER(PacketCode) LIKE '%reeferfuellevelpercent%' THEN VALFLOAT
                WHEN LOWER(PacketCode) LIKE '%adbluelevelpercent%' THEN VALFLOAT
                WHEN LOWER(Titolo) LIKE '%carburante (%)%' THEN VALFLOAT
                WHEN LOWER(Titolo) LIKE '%adblue level (%)%' THEN VALFLOAT

                WHEN LOWER(PacketCode) LIKE '%fuellevel%' AND Y_Titolo = 'L' THEN VALFLOAT
                WHEN LOWER(PacketCode) LIKE '%reeferfuellevel%' AND Y_Titolo = 'L' THEN VALFLOAT

                ELSE NULL
            END AS FuelValue,

            CASE 
                WHEN LOWER(PacketCode) LIKE '%fuellevelpercent%' THEN '%'
                WHEN LOWER(PacketCode) LIKE '%fuellevel2percent%' THEN '%'
                WHEN LOWER(PacketCode) LIKE '%fuellevel%' AND Y_Titolo = '%' THEN '%'
                WHEN LOWER(PacketCode) LIKE '%lnglevelpercent%' THEN '%'
                WHEN LOWER(PacketCode) LIKE '%cnglevelpercent%' THEN '%'
                WHEN LOWER(PacketCode) LIKE '%reeferfuellevelpercent%' THEN '%'
                WHEN LOWER(PacketCode) LIKE '%adbluelevelpercent%' THEN '%'
                WHEN LOWER(Titolo) LIKE '%carburante (%)%' THEN '%'
                WHEN LOWER(Titolo) LIKE '%adblue level (%)%' THEN '%'

                WHEN LOWER(PacketCode) LIKE '%fuellevel%' AND Y_Titolo = 'L' THEN 'L'
                WHEN LOWER(PacketCode) LIKE '%reeferfuellevel%' AND Y_Titolo = 'L' THEN 'L'

                ELSE NULL
            END AS FuelUnit,

            CASE 
                WHEN LOWER(PacketCode) LIKE '%fuellevelpercent%' THEN 1
                WHEN LOWER(PacketCode) LIKE '%fuellevel2percent%' THEN 1
                WHEN LOWER(PacketCode) LIKE '%fuellevel%' AND Y_Titolo = '%' THEN 1
                WHEN LOWER(PacketCode) LIKE '%lnglevelpercent%' THEN 1
                WHEN LOWER(PacketCode) LIKE '%cnglevelpercent%' THEN 1
                WHEN LOWER(PacketCode) LIKE '%reeferfuellevelpercent%' THEN 1
                WHEN LOWER(PacketCode) LIKE '%adbluelevelpercent%' THEN 1
                WHEN LOWER(Titolo) LIKE '%carburante (%)%' THEN 1
                WHEN LOWER(Titolo) LIKE '%adblue level (%)%' THEN 1

                WHEN LOWER(PacketCode) LIKE '%fuellevel%' AND Y_Titolo = 'L' THEN 0
                WHEN LOWER(PacketCode) LIKE '%reeferfuellevel%' AND Y_Titolo = 'L' THEN 0

                ELSE NULL
            END AS IsPercentage
        FROM Enriched E
    )

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
    FROM FuelDetection
    WHERE FuelValue IS NOT NULL
    ORDER BY ID_MEZZO, DATAEVENTO, ID_EVENT;
    """

    conn = get_connection(server, "master")
    df = pd.read_sql(
        query,
        conn,
        params=[id_mezzo, targa, date_from, date_to]
    )
    conn.close()

    return df
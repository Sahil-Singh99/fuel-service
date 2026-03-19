from dotenv import load_dotenv
import os

load_dotenv()

DB_DRIVER = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")
DEFAULT_DATABASE = os.getenv("DB_DATABASE", "master")

DOMAIN_CONFIG = {
    "WAY6223": {
        "server": "WAY6223",
        "story_db": "WAY6223_STORY",
        "core_db": "WAY6223_CORE",
    },
    "WAY212": {
        "server": "WAY212",
        "story_db": "WAY212_STORY",
        "core_db": "WAY212_CORE",
    },
    "WAY202": {
        "server": "WAY202",
        "story_db": "WAY202_STORY",
        "core_db": "WAY202_CORE",
    },
}
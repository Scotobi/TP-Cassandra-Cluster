"""
Alimentation de la table métier du TP2 (velib_cluster.stations_velib)
à partir de l'API Open Data Vélib' de la Ville de Paris.

Différences avec le script fourni :
  - le niveau de cohérence QUORUM est réellement appliqué (le driver Python
    utilise LOCAL_ONE par défaut, pas QUORUM) ;
  - le driver ne parle qu'à cass1 via 127.0.0.1:9042 (WhiteListRoundRobinPolicy) :
    les IP internes Docker de cass2/cass3 ne sont pas joignables depuis Windows/WSL ;
  - pagination de l'API (100 enregistrements max par appel).

Usage :
    pip install "cassandra-driver>=3.29" requests
    python scripts/import_velib.py            # 100 stations
    python scripts/import_velib.py --nb 300   # 300 stations
"""
import argparse
from datetime import datetime, timezone

import requests
from cassandra import ConsistencyLevel
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile
from cassandra.policies import WhiteListRoundRobinPolicy

API_URL = (
    "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/"
    "velib-disponibilite-en-temps-reel/records"
)


def fetch_stations(nb: int) -> list[dict]:
    """Récupère `nb` stations en paginant (limite API : 100 par appel)."""
    results, offset = [], 0
    while len(results) < nb:
        limit = min(100, nb - len(results))
        r = requests.get(API_URL, params={"limit": limit, "offset": offset}, timeout=30)
        r.raise_for_status()
        page = r.json().get("results", [])
        if not page:
            break
        results.extend(page)
        offset += len(page)
    return results


def parse_date(value) -> datetime:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return datetime.now(timezone.utc)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9042)
    parser.add_argument("--nb", type=int, default=100, help="nombre de stations à importer")
    args = parser.parse_args()

    profile = ExecutionProfile(
        load_balancing_policy=WhiteListRoundRobinPolicy([args.host]),
        consistency_level=ConsistencyLevel.QUORUM,
        request_timeout=30,
    )
    cluster = Cluster(
        [args.host],
        port=args.port,
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
    )
    session = cluster.connect("velib_cluster")

    insert = session.prepare(
        """
        INSERT INTO stations_velib (station_id, nom_station, capacite,
                                    vatiques_disponibles, bornes_disponibles,
                                    derniere_mise_a_jour)
        VALUES (?, ?, ?, ?, ?, ?)
        """
    )
    insert.consistency_level = ConsistencyLevel.QUORUM

    stations = fetch_stations(args.nb)
    print(f"Insertion de {len(stations)} stations dans Cassandra (CL = QUORUM)...")

    for rec in stations:
        session.execute(
            insert,
            (
                str(rec.get("stationcode")),
                rec.get("name"),
                int(rec.get("capacity") or 0),
                int(rec.get("numbikesavailable") or 0),
                int(rec.get("numdocksavailable") or 0),
                parse_date(rec.get("duedate")),
            ),
        )

    total = session.execute("SELECT count(*) FROM stations_velib").one()[0]
    print(f"Insertion terminée. Lignes dans stations_velib : {total}")
    cluster.shutdown()


if __name__ == "__main__":
    main()

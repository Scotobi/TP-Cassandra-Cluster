# TP3 — Cluster Cassandra : réplication et tolérance aux pannes

M2 Big Data & IA — Données distribuées · SUP DE VINCI Rennes · travail individuel

Ce dépôt reprend la **table métier du TP2** (`velib_cluster.stations_velib`, disponibilités des stations Vélib' en temps réel) et l'exécute sur un **cluster Cassandra à 3 nœuds** (`cass1`, `cass2`, `cass3`) pour observer le partitionnement, la réplication (RF = 3), les niveaux de cohérence `ONE` / `QUORUM` / `ALL`, puis la panne et le retour d'un nœud.

Le compte-rendu se trouve dans [`docs/compte-rendu.md`](docs/compte-rendu.md).

## Arborescence

```text
TP-Cassandra-Cluster/
├── docker-compose.yml          # cluster tp2-cluster : dc1, rack1..3, cass1 = seed
├── README.md
├── cql/
│   └── 01_schema.cql           # keyspace velib_cluster (NetworkTopologyStrategy, dc1: 3) + table du TP2
├── scripts/
│   ├── import_velib.py         # alimentation depuis l'API Open Data Vélib' (écritures en QUORUM)
│   ├── run_tp.sh               # scénario complet (WSL / Linux / Git Bash)
│   └── run_tp.ps1              # scénario complet (PowerShell)
└── docs/
    ├── compte-rendu.md
    ├── preuves/                # sorties texte générées par run_tp.*
    └── captures/               # captures d'écran
```

## Prérequis

- Docker Desktop (ou Docker Engine) avec **au moins 4 Go de RAM** alloués (3 × 512 Mo de heap + overhead JVM). Sous WSL2, vérifier la limite `memory` dans `.wslconfig`.
- Python 3.10+ et les dépendances :

```bash
pip install "cassandra-driver>=3.29" requests
```

## Lancer le scénario automatiquement

```bash
# WSL / Linux / Git Bash
bash scripts/run_tp.sh

# PowerShell
powershell -ExecutionPolicy Bypass -File scripts\run_tp.ps1
```

Ajouter `--no-import` (bash) ou `-NoImport` (PowerShell) si la table contient déjà les données du TP2.

Le script enchaîne toutes les étapes du sujet et écrit chaque preuve dans `docs/preuves/` :

| Fichier | Étape |
|---|---|
| `01_cluster_3_noeuds.txt` | démarrage cass1 → cass1+cass2 → 3 nœuds, `nodetool status` à chaque fois |
| `02_configuration_cluster.txt` | `nodetool describecluster`, conteneurs |
| `03_table_metier.txt` | keyspace, table, nombre de lignes, échantillon, station de test |
| `04_replication_distribution.txt` | `repair`, `status` du keyspace (Owns 100 %), `getendpoints`, `token()`, `ring` |
| `05_coherence_3_noeuds*.txt` | lectures ONE / QUORUM / ALL (+ traces) |
| `06_panne_cass3.txt` | `docker stop cass3`, `nodetool status` (cass3 = DN) |
| `07_lectures_pendant_panne*.txt` | lectures ONE / QUORUM / ALL, écritures ALL (échec) et QUORUM (succès), hints en attente |
| `08_retour_cass3.txt` | `docker start cass3`, retour en UN, logs du hinted handoff |
| `09_verification_finale*.txt` | lectures finales, lecture directe sur cass3 de la donnée écrite pendant la panne |

Le script affiche chaque commande dans le terminal : il suffit de faire une capture d'écran à chaque bannière pour obtenir les 7 captures demandées.

## Commandes manuelles (équivalent pas à pas)

```bash
# 1. Démarrage un par un (attendre UN avant le suivant)
docker compose up -d cass1 && docker exec cass1 nodetool status
docker compose up -d cass2 && docker exec cass1 nodetool status
docker compose up -d cass3 && docker exec cass1 nodetool status
docker exec cass1 nodetool describecluster

# 2. Table métier + RF = 3
docker exec -i cass1 cqlsh < cql/01_schema.cql
python scripts/import_velib.py --nb 100
docker exec cass1 nodetool repair -full velib_cluster
docker exec cass1 nodetool status velib_cluster
docker exec cass1 nodetool getendpoints velib_cluster stations_velib <station_id>
docker exec cass1 nodetool ring velib_cluster

# 3. Cohérence (dans docker exec -it cass1 cqlsh)
#    CONSISTENCY ONE;    SELECT * FROM velib_cluster.stations_velib WHERE station_id='<station_id>';
#    CONSISTENCY QUORUM; (même SELECT)
#    CONSISTENCY ALL;    (même SELECT)

# 4. Panne / retour
docker stop cass3  && docker exec cass1 nodetool status
#    -> refaire les 3 lectures
docker start cass3 && docker exec cass1 nodetool status
```

## Nettoyage

```bash
docker compose down        # arrête et supprime les conteneurs, conserve les données
docker compose down -v     # supprime aussi les volumes (perte des données Cassandra)
```

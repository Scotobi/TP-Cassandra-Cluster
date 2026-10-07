#!/usr/bin/env bash
# =====================================================================
# TP3 — Cluster Cassandra : réplication et tolérance aux pannes
# Exécute tout le scénario et enregistre chaque preuve dans docs/preuves/
#
# Usage (depuis la racine du dépôt, WSL / Linux / Git Bash) :
#   bash scripts/run_tp.sh            # scénario complet
#   bash scripts/run_tp.sh --no-import  # si la table contient déjà les données du TP2
# =====================================================================
set -uo pipefail
cd "$(dirname "$0")/.."

PREUVES=docs/preuves
mkdir -p "$PREUVES"
KS=velib_cluster
TABLE=stations_velib
DO_IMPORT=1
[[ "${1:-}" == "--no-import" ]] && DO_IMPORT=0

banner() { printf '\n\033[1;36m==================== %s ====================\033[0m\n' "$*"; }
# run <fichier> <commande...> : affiche la commande + sa sortie, et l'enregistre
run() {
  local f="$PREUVES/$1"; shift
  { echo "\$ $*"; "$@" 2>&1; echo; } | tee -a "$f"
}
cql() { local node=$1; shift; docker exec "$node" cqlsh -e "$*"; }

wait_un() {   # wait_un <nb_noeuds_UN_attendus>
  echo -n "Attente de $1 nœud(s) UN"
  until [ "$(docker exec cass1 nodetool status 2>/dev/null | grep -c '^UN')" -ge "$1" ]; do
    echo -n "."; sleep 5
  done; echo " OK"
}
wait_cql() {  # wait_cql <noeud>
  echo -n "Attente de CQL sur $1"
  until docker exec "$1" cqlsh -e "SELECT now() FROM system.local" >/dev/null 2>&1; do
    echo -n "."; sleep 5
  done; echo " OK"
}

rm -f "$PREUVES"/*.txt

# ---------------------------------------------------------------------
banner "1. Démarrage progressif du cluster"
docker compose up -d cass1;  wait_un 1; run 01_cluster_3_noeuds.txt docker exec cass1 nodetool status
docker compose up -d cass2;  wait_un 2; run 01_cluster_3_noeuds.txt docker exec cass1 nodetool status
docker compose up -d cass3;  wait_un 3; run 01_cluster_3_noeuds.txt docker exec cass1 nodetool status
wait_cql cass1

banner "2. Configuration du cluster"
run 02_configuration_cluster.txt docker exec cass1 nodetool describecluster
run 02_configuration_cluster.txt docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# ---------------------------------------------------------------------
banner "3. Table métier du TP2 (keyspace + RF=3)"
docker exec -i cass1 cqlsh < cql/01_schema.cql
if [ "$DO_IMPORT" -eq 1 ]; then
  python3 scripts/import_velib.py --nb 100 2>&1 | tee -a "$PREUVES/03_table_metier.txt" \
    || { echo "Import Python impossible : vérifie 'pip install cassandra-driver requests'"; exit 1; }
fi
run 03_table_metier.txt cql cass1 "DESCRIBE KEYSPACE $KS;"
run 03_table_metier.txt cql cass1 "SELECT count(*) FROM $KS.$TABLE;"
run 03_table_metier.txt cql cass1 "SELECT station_id, nom_station, capacite, vatiques_disponibles, bornes_disponibles FROM $KS.$TABLE LIMIT 5;"

# Donnée réelle utilisée pour tous les tests
ID=$(docker exec cass1 cqlsh -e "SELECT station_id FROM $KS.$TABLE LIMIT 1;" | sed -n '4p' | tr -d ' \r')
echo "Station de test : station_id = '$ID'" | tee -a "$PREUVES/03_table_metier.txt"
echo "$ID" > "$PREUVES/station_test.id"

# ---------------------------------------------------------------------
banner "4. Réplication RF=3 : repair + distribution"
run 04_replication_distribution.txt docker exec cass1 nodetool repair -full $KS
run 04_replication_distribution.txt docker exec cass1 nodetool status $KS
run 04_replication_distribution.txt docker exec cass1 nodetool getendpoints $KS $TABLE "$ID"
run 04_replication_distribution.txt cql cass1 "SELECT token(station_id), station_id, nom_station FROM $KS.$TABLE LIMIT 5;"
run 04_replication_distribution.txt docker exec cass1 nodetool ring $KS
run 04_replication_distribution.txt docker exec cass1 nodetool describering $KS

# ---------------------------------------------------------------------
lectures() {  # lectures <fichier> <noeud_coordinateur>
  for CL in ONE QUORUM ALL; do
    run "$1" cql "$2" "CONSISTENCY $CL; SELECT * FROM $KS.$TABLE WHERE station_id='$ID';"
    # même lecture tracée (nœuds sollicités) -> fichier séparé
    { echo "### CONSISTENCY $CL (TRACING ON)"; cql "$2" "CONSISTENCY $CL; TRACING ON; SELECT * FROM $KS.$TABLE WHERE station_id='$ID';" 2>&1; echo; } \
      >> "$PREUVES/${1%.txt}_trace.txt"
  done
}

banner "5. Tests de cohérence ONE / QUORUM / ALL (3 nœuds UN)"
lectures 05_coherence_3_noeuds.txt cass1

# ---------------------------------------------------------------------
banner "6. Panne de cass3"
run 06_panne_cass3.txt docker stop cass3
sleep 10
run 06_panne_cass3.txt docker ps -a --format "table {{.Names}}\t{{.Status}}"
run 06_panne_cass3.txt docker exec cass1 nodetool status

banner "7. Lectures pendant la panne"
lectures 07_lectures_pendant_panne.txt cass1
# écriture pendant la panne : QUORUM OK (2/3), ALL KO
run 07_lectures_pendant_panne.txt cql cass1 "CONSISTENCY ALL; INSERT INTO $KS.$TABLE (station_id, nom_station, capacite, vatiques_disponibles, bornes_disponibles, derniere_mise_a_jour) VALUES ('99998', 'Station Test ALL', 30, 15, 15, toTimestamp(now()));"
run 07_lectures_pendant_panne.txt cql cass1 "CONSISTENCY QUORUM; INSERT INTO $KS.$TABLE (station_id, nom_station, capacite, vatiques_disponibles, bornes_disponibles, derniere_mise_a_jour) VALUES ('99999', 'Station Test Panne', 30, 15, 15, toTimestamp(now()));"
run 07_lectures_pendant_panne.txt cql cass1 "CONSISTENCY QUORUM; SELECT * FROM $KS.$TABLE WHERE station_id='99999';"
run 07_lectures_pendant_panne.txt docker exec cass1 nodetool listpendinghints

# ---------------------------------------------------------------------
banner "8. Redémarrage de cass3"
run 08_retour_cass3.txt docker start cass3
sleep 5
run 08_retour_cass3.txt docker exec cass1 nodetool status   # état intermédiaire (encore DN pendant le démarrage)
wait_un 3
wait_cql cass3
sleep 20   # laisse le temps au hinted handoff
run 08_retour_cass3.txt docker exec cass1 nodetool status
run 08_retour_cass3.txt docker ps --format "table {{.Names}}\t{{.Status}}"
{ echo "\$ docker logs cass1 | grep -i hint"; docker logs cass1 2>&1 | grep -i hint | tail -5; echo; } | tee -a "$PREUVES/08_retour_cass3.txt"

# ---------------------------------------------------------------------
banner "9. Vérification finale des données"
run 09_verification_finale.txt docker exec cass1 nodetool status $KS
lectures 09_verification_finale.txt cass1
# lecture directe sur cass3 à ONE : la station écrite pendant la panne est là
run 09_verification_finale.txt cql cass3 "CONSISTENCY ONE; SELECT * FROM $KS.$TABLE WHERE station_id='99999';"
{ echo "### Lecture 99999 sur cass3, CL ONE (TRACING ON)"; cql cass3 "CONSISTENCY ONE; TRACING ON; SELECT * FROM $KS.$TABLE WHERE station_id='99999';" 2>&1; } \
  >> "$PREUVES/09_verification_finale_trace.txt"
run 09_verification_finale.txt cql cass3 "CONSISTENCY ALL; SELECT count(*) FROM $KS.$TABLE;"

banner "Terminé — preuves dans $PREUVES/"
echo "Nettoyage quand tu as fini :  docker compose down   (ou  docker compose down -v  pour supprimer les volumes)"

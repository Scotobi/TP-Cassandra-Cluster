# =====================================================================
# TP3 — Cluster Cassandra : réplication et tolérance aux pannes
# Version PowerShell (Windows + Docker Desktop) de scripts/run_tp.sh
#
# Usage (depuis la racine du dépôt) :
#   powershell -ExecutionPolicy Bypass -File scripts\run_tp.ps1
#   powershell -ExecutionPolicy Bypass -File scripts\run_tp.ps1 -NoImport
# =====================================================================
param([switch]$NoImport)

Set-Location (Join-Path $PSScriptRoot "..")
$PYTHON = Join-Path $PSScriptRoot "..\.venv\Scripts\python.exe"
if (-not (Test-Path $PYTHON)) { $PYTHON = "python" }
$PREUVES = "docs/preuves"
New-Item -ItemType Directory -Force $PREUVES | Out-Null
Remove-Item "$PREUVES/*.txt" -ErrorAction SilentlyContinue
$KS = "velib_cluster"; $TABLE = "stations_velib"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

function Banner($t) { Write-Host "`n==================== $t ====================" -ForegroundColor Cyan }

# Run <fichier> <exe> <args...> : affiche la commande + sa sortie, et l'enregistre
function Run($file, $exe) {
  $argv = $args
  $out = @("`$ $exe $($argv -join ' ')") + (& $exe @argv 2>&1 | ForEach-Object { "$_" }) + ""
  $out | Tee-Object -FilePath "$PREUVES/$file" -Append
}
function Cql($node, $query) { docker exec $node cqlsh -e $query 2>&1 | ForEach-Object { "$_" } }
function RunCql($file, $node, $query) {
  $out = @("`$ cqlsh ($node) -e `"$query`"") + (Cql $node $query) + ""
  $out | Tee-Object -FilePath "$PREUVES/$file" -Append
}

function Wait-UN($n) {
  Write-Host -NoNewline "Attente de $n noeud(s) UN"
  while (((docker exec cass1 nodetool status 2>$null) -match '^UN').Count -lt $n) { Write-Host -NoNewline "."; Start-Sleep 5 }
  Write-Host " OK"
}
function Wait-Cql($node) {
  Write-Host -NoNewline "Attente de CQL sur $node"
  while ($true) { docker exec $node cqlsh -e "SELECT now() FROM system.local" *> $null; if ($LASTEXITCODE -eq 0) { break }; Write-Host -NoNewline "."; Start-Sleep 5 }
  Write-Host " OK"
}

function Lectures($file, $node) {
  foreach ($cl in "ONE", "QUORUM", "ALL") {
    RunCql $file $node "CONSISTENCY $cl; SELECT * FROM $KS.$TABLE WHERE station_id='$ID';"
    $trace = $file -replace '\.txt$', '_trace.txt'
    (@("### CONSISTENCY $cl (TRACING ON)") + (Cql $node "CONSISTENCY $cl; TRACING ON; SELECT * FROM $KS.$TABLE WHERE station_id='$ID';") + "") |
      Out-File -Append -Encoding utf8 "$PREUVES/$trace"
  }
}

# ---------------------------------------------------------------------
Banner "1. Demarrage progressif du cluster"
docker compose up -d cass1; Wait-UN 1; Run 01_cluster_3_noeuds.txt docker exec cass1 nodetool status
docker compose up -d cass2; Wait-UN 2; Run 01_cluster_3_noeuds.txt docker exec cass1 nodetool status
docker compose up -d cass3; Wait-UN 3; Run 01_cluster_3_noeuds.txt docker exec cass1 nodetool status
Wait-Cql cass1

Banner "2. Configuration du cluster"
Run 02_configuration_cluster.txt docker exec cass1 nodetool describecluster
Run 02_configuration_cluster.txt docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# ---------------------------------------------------------------------
Banner "3. Table metier du TP2 (keyspace + RF=3)"
Get-Content cql/01_schema.cql -Raw | docker exec -i cass1 cqlsh
if (-not $NoImport) {
  & $PYTHON scripts/import_velib.py --nb 100 2>&1 | Tee-Object -FilePath "$PREUVES/03_table_metier.txt" -Append
  if ($LASTEXITCODE -ne 0) { Write-Host "Import Python impossible : pip install cassandra-driver requests"; exit 1 }
}
RunCql 03_table_metier.txt cass1 "DESCRIBE KEYSPACE $KS;"
RunCql 03_table_metier.txt cass1 "SELECT count(*) FROM $KS.$TABLE;"
RunCql 03_table_metier.txt cass1 "SELECT station_id, nom_station, capacite, vatiques_disponibles, bornes_disponibles FROM $KS.$TABLE LIMIT 5;"

$ID = ((docker exec cass1 cqlsh -e "SELECT station_id FROM $KS.$TABLE LIMIT 1;")[3]).Trim()
"Station de test : station_id = '$ID'" | Tee-Object -FilePath "$PREUVES/03_table_metier.txt" -Append
$ID | Out-File -Encoding ascii "$PREUVES/station_test.id"

# ---------------------------------------------------------------------
Banner "4. Replication RF=3 : repair + distribution"
Run 04_replication_distribution.txt docker exec cass1 nodetool repair -full $KS
Run 04_replication_distribution.txt docker exec cass1 nodetool status $KS
Run 04_replication_distribution.txt docker exec cass1 nodetool getendpoints $KS $TABLE $ID
RunCql 04_replication_distribution.txt cass1 "SELECT token(station_id), station_id, nom_station FROM $KS.$TABLE LIMIT 5;"
Run 04_replication_distribution.txt docker exec cass1 nodetool ring $KS
Run 04_replication_distribution.txt docker exec cass1 nodetool describering $KS

# ---------------------------------------------------------------------
Banner "5. Tests de coherence ONE / QUORUM / ALL (3 noeuds UN)"
Lectures 05_coherence_3_noeuds.txt cass1

# ---------------------------------------------------------------------
Banner "6. Panne de cass3"
Run 06_panne_cass3.txt docker stop cass3
Start-Sleep 10
Run 06_panne_cass3.txt docker ps -a --format "table {{.Names}}\t{{.Status}}"
Run 06_panne_cass3.txt docker exec cass1 nodetool status

Banner "7. Lectures pendant la panne"
Lectures 07_lectures_pendant_panne.txt cass1
RunCql 07_lectures_pendant_panne.txt cass1 "CONSISTENCY ALL; INSERT INTO $KS.$TABLE (station_id, nom_station, capacite, vatiques_disponibles, bornes_disponibles, derniere_mise_a_jour) VALUES ('99998', 'Station Test ALL', 30, 15, 15, toTimestamp(now()));"
RunCql 07_lectures_pendant_panne.txt cass1 "CONSISTENCY QUORUM; INSERT INTO $KS.$TABLE (station_id, nom_station, capacite, vatiques_disponibles, bornes_disponibles, derniere_mise_a_jour) VALUES ('99999', 'Station Test Panne', 30, 15, 15, toTimestamp(now()));"
RunCql 07_lectures_pendant_panne.txt cass1 "CONSISTENCY QUORUM; SELECT * FROM $KS.$TABLE WHERE station_id='99999';"
Run 07_lectures_pendant_panne.txt docker exec cass1 nodetool listpendinghints

# ---------------------------------------------------------------------
Banner "8. Redemarrage de cass3"
Run 08_retour_cass3.txt docker start cass3
Start-Sleep 5
Run 08_retour_cass3.txt docker exec cass1 nodetool status
Wait-UN 3
Wait-Cql cass3
Start-Sleep 20
Run 08_retour_cass3.txt docker exec cass1 nodetool status
Run 08_retour_cass3.txt docker ps --format "table {{.Names}}\t{{.Status}}"
(@('$ docker logs cass1 | grep -i hint') + (docker logs cass1 2>&1 | Select-String -Pattern 'hint' | Select-Object -Last 5 | ForEach-Object { "$_" }) + "") |
  Tee-Object -FilePath "$PREUVES/08_retour_cass3.txt" -Append

# ---------------------------------------------------------------------
Banner "9. Verification finale des donnees"
Run 09_verification_finale.txt docker exec cass1 nodetool status $KS
Lectures 09_verification_finale.txt cass1
RunCql 09_verification_finale.txt cass3 "CONSISTENCY ONE; SELECT * FROM $KS.$TABLE WHERE station_id='99999';"
(@("### Lecture 99999 sur cass3, CL ONE (TRACING ON)") + (Cql cass3 "CONSISTENCY ONE; TRACING ON; SELECT * FROM $KS.$TABLE WHERE station_id='99999';")) |
  Out-File -Append -Encoding utf8 "$PREUVES/09_verification_finale_trace.txt"
RunCql 09_verification_finale.txt cass3 "CONSISTENCY ALL; SELECT count(*) FROM $KS.$TABLE;"

Banner "Termine - preuves dans $PREUVES/"
Write-Host "Nettoyage : docker compose down   (ou docker compose down -v pour supprimer les volumes)"

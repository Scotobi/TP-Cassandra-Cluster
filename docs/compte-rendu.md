# Compte-rendu — TP3 : Cluster Cassandra, réplication et tolérance aux pannes

**Auteur :** William · M2 Big Data & IA — SUP DE VINCI Rennes
**Technologies :** Apache Cassandra 4.1 · Docker Compose · CQL · Python (`cassandra-driver`)
**Table métier :** `velib_cluster.stations_velib` (TP2 — API Open Data Vélib')

> Les sorties brutes de chaque étape sont dans `docs/preuves/` (générées par `scripts/run_tp.sh` ou `run_tp.ps1`) et les captures d'écran dans `docs/captures/`.
> Les valeurs ci-dessous correspondent à l'exécution du 7 octobre 2026. Les preuves brutes associées sont conservées dans `docs/preuves/`.

---

## 1. Architecture du cluster

```text
                 Cluster tp2-cluster
                         │
                        dc1
                         │
          ┌──────────────┼──────────────┐
        rack1          rack2          rack3
          │              │              │
        cass1          cass2          cass3
       (seed)
```

| Paramètre | Valeur | Source |
|---|---|---|
| Nom du cluster | `tp2-cluster` | `CASSANDRA_CLUSTER_NAME` |
| Datacenter | `dc1` | `CASSANDRA_DC` |
| Racks | `rack1`, `rack2`, `rack3` (un nœud par rack) | `CASSANDRA_RACK` |
| Seed | `cass1` | `CASSANDRA_SEEDS` |
| Snitch | `GossipingPropertyFileSnitch` | `CASSANDRA_ENDPOINT_SNITCH` |
| vnodes | 16 tokens par nœud → 48 plages sur l'anneau | `CASSANDRA_NUM_TOKENS` |
| Accès client | `cass1` exposé sur `localhost:9042` | `ports` |
| Réseau | `cass-net` (bridge Docker) | `networks` |

Les nœuds ont été démarrés **un par un** (`cass1`, puis `cass2`, puis `cass3`), en attendant l'état `UN` avant de lancer le suivant : un nœud qui rejoint le cluster (`UJ`) récupère sa part de l'anneau en streaming, et faire rejoindre deux nœuds en même temps peut créer des conflits d'attribution de tokens.

### Question 1 — État après démarrage des trois nœuds

📸 Capture 1 : `docs/captures/01_cluster_3_noeuds.png` · Preuve : `docs/preuves/01_cluster_3_noeuds.txt`

```text
Datacenter: dc1
UN  172.18.0.4  152.35 KiB  16  100.0%  1c3b0638-d0c9-4b05-9a3f-01a3676a6e64  rack3
UN  172.18.0.3  158.13 KiB  16  100.0%  d248f927-cc84-49ea-a083-0bd8834e0477  rack2
UN  172.18.0.2  135.82 KiB  16  100.0%  1d1e5965-48c9-4b3d-9f4c-caa857d686a2  rack1
```

- **Nombre de nœuds :** 3 (`cass1`, `cass2`, `cass3`).
- **État :** `UN` pour les trois (*Up / Normal* : nœud joignable et propriétaire de ses plages de tokens).
- **Datacenter :** tous dans `dc1` (en-tête `Datacenter: dc1`).
- **Racks :** `cass1` → `rack1`, `cass2` → `rack2`, `cass3` → `rack3` (colonne `Rack`).

Les commandes `up` ont été lancées dans l'ordre `cass1`, `cass2`, `cass3`. Toutefois, les preuves de cette exécution montrent déjà les trois nœuds `UN` dès le premier contrôle : les conteneurs étaient donc déjà présents et le scénario ne documente pas une montée progressive de 1 à 3 nœuds. Aucun état `UJ` n'a été relevé dans les sorties conservées.

### Question 2 — Architecture obtenue

`nodetool describecluster` (preuve `02_configuration_cluster.txt`) confirme le nom `tp2-cluster`, le partitionneur `Murmur3Partitioner`, le snitch `GossipingPropertyFileSnitch` et **une seule version de schéma** partagée par les 3 nœuds (le schéma est bien synchronisé).

| Niveau | Définition | Dans ce TP |
|---|---|---|
| **Cluster** | Ensemble de tous les nœuds qui partagent le même anneau de tokens et le même nom de cluster. C'est le périmètre de la base. | `tp2-cluster` |
| **Datacenter** | Groupe logique de nœuds (souvent un site ou une région). La réplication se configure **par datacenter** (`'dc1': 3`). | `dc1` |
| **Rack** | Sous-groupe d'un datacenter qui partage un point de défaillance commun (baie, alimentation, zone de disponibilité). Cassandra répartit les réplicas sur des racks différents. | `rack1`, `rack2`, `rack3` |
| **Node** | Une instance Cassandra (ici un conteneur) qui stocke une partie des données et peut coordonner n'importe quelle requête. | `cass1`, `cass2`, `cass3` |

Cassandra est **leaderless** : il n'y a pas de maître. Le seed (`cass1`) sert uniquement de point d'entrée au protocole de gossip lors du démarrage ; une fois le cluster formé, les trois nœuds jouent le même rôle.

---

## 2. Table métier du TP2

📸 Capture 2 : `docs/captures/02_table_metier.png` · Preuve : `docs/preuves/03_table_metier.txt`

### Question 3 — Présentation de la table

```sql
CREATE TABLE velib_cluster.stations_velib (
  station_id            text,
  nom_station           text,
  capacite              int,
  vatiques_disponibles  int,
  bornes_disponibles    int,
  derniere_mise_a_jour  timestamp,
  PRIMARY KEY (station_id)
);
```

| Élément | Valeur |
|---|---|
| Keyspace | `velib_cluster` |
| Table | `stations_velib` |
| Colonnes principales | `station_id`, `nom_station`, `capacite`, `vatiques_disponibles` (vélos disponibles), `bornes_disponibles`, `derniere_mise_a_jour` |
| Partition key | `station_id` |
| Clustering key | aucune |

Chaque station est **une partition contenant une seule ligne** : la table conserve l'état le plus récent de chaque station (un nouvel `INSERT` sur le même `station_id` écrase l'ancien, c'est un *upsert*). La requête métier principale, « disponibilité d'une station donnée », se fait donc par partition key, ce qui est le cas d'accès optimal dans Cassandra. Cette exécution a importé 100 stations depuis l'API.

Les données sont chargées par `scripts/import_velib.py` depuis l'API Open Data de la Ville de Paris, avec des écritures en `QUORUM`.

L'import en `QUORUM` a inséré **100 stations** (`SELECT count(*) = 100`). L'échantillon retourné par Cassandra est :

| station_id | nom_station | capacite | vatiques_disponibles | bornes_disponibles |
|---|---|---:|---:|---:|
| `15047` | Morillons - Dantzig | 52 | 1 | 51 |
| `12151` | Gare de Lyon - Place Louis Armand | 60 | 42 | 16 |
| `14013` | Liard - Amiral Mouchez | 28 | 1 | 25 |
| `9020` | Toudouze - Clauzel | 21 | 3 | 18 |
| `18111` | Félix Ziem - Armand Gauthier | 25 | 1 | 23 |

Station de test utilisée dans tout le TP : `station_id = '15047'`.

---

## 3. Réplication (RF = 3)

```sql
ALTER KEYSPACE velib_cluster
WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3};
```

```bash
docker exec cass1 nodetool repair -full velib_cluster   # recopie les données existantes sur les nouveaux réplicas
docker exec cass1 nodetool status velib_cluster          # Owns (effective) = 100 % sur chaque nœud
```

`NetworkTopologyStrategy` est la stratégie adaptée à un cluster réel : elle connaît la topologie (datacenters et racks, fournie par le snitch) et place les réplicas sur des racks différents. `SimpleStrategy` ignore la topologie et n'est utilisée que pour les tests mono-datacenter.

Le `repair` est nécessaire quand on augmente le RF d'un keyspace qui contient déjà des données : `ALTER KEYSPACE` change seulement la règle de placement, il ne déplace pas les données déjà écrites.

### Question 4 — Que signifie RF = 3 pour une partition ?

Pour une station donnée (par exemple `station_id = '<id de test>'`), **RF = 3 signifie que la partition existe en 3 copies, sur 3 nœuds différents** du datacenter `dc1`. Comme le cluster compte exactement 3 nœuds, **chaque nœud possède une copie de toutes les stations** : c'est pour cela que `nodetool status velib_cluster` affiche `Owns (effective) = 100 %` pour chaque nœud, et que `nodetool getendpoints` renvoie les 3 adresses IP.

| | Partitionnement | Réplication |
|---|---|---|
| Question traitée | **Où** est rangée une donnée ? | **Combien de copies** existe-t-il ? |
| Mécanisme | Hash Murmur3 de la partition key → token → nœud propriétaire de la plage | Le keyspace définit RF ; les RF−1 nœuds suivants sur l'anneau (racks distincts) reçoivent une copie |
| Objectif | Répartir la charge et le volume (scalabilité horizontale) | Tolérance aux pannes et disponibilité |
| Défini au niveau | Table (choix de la partition key) | Keyspace (`replication = {...}`) |

Les deux sont complémentaires : le partitionnement découpe les données entre nœuds, la réplication duplique chaque morceau. Avec 3 nœuds et RF = 3 le partitionnement existe toujours (chaque nœud est *propriétaire principal* d'environ 1/3 des tokens), mais la réplication rend chaque nœud dépositaire de 100 % des données.

---

## 4. Distribution des données

Preuve : `docs/preuves/04_replication_distribution.txt`

Pour `station_id = '15047'`, `nodetool getendpoints` retourne les trois réplicas `172.18.0.3`, `172.18.0.2` et `172.18.0.4` (racks 2, 1 et 3). L'extrait de `SELECT token(station_id), station_id, nom_station ... LIMIT 5` est :

| Token | station_id | nom_station |
|---:|---|---|
| -9181437804237570319 | `15047` | Morillons - Dantzig |
| -9174480588412330867 | `12151` | Gare de Lyon - Place Louis Armand |
| -9155749074501570029 | `14013` | Liard - Amiral Mouchez |
| -8638787176979541571 | `9020` | Toudouze - Clauzel |
| -8633603814511203154 | `18111` | Félix Ziem - Armand Gauthier |

`nodetool ring velib_cluster` montre les **48 tokens** (16 vnodes × 3 nœuds) répartis sur l'anneau de −2⁶³ à 2⁶³−1, chaque nœud possédant 16 plages non contiguës.

### Question 5 — Chemin d'une donnée

Exemple avec la station de test :

```text
Partition key   station_id = '<id>'
      ↓
Hash            Murmur3Partitioner : murmur3('<id>')
      ↓
Token           un entier 64 bits, ex. token('<id>') = <valeur vue avec SELECT token(...)>
      ↓
Nœud(s)         le nœud dont la plage de tokens (vnode) contient ce token
responsable(s)  est le propriétaire principal de la partition
      ↓
Réplicas        NetworkTopologyStrategy parcourt l'anneau dans le sens horaire
                et retient les nœuds suivants situés sur des racks différents
                jusqu'à obtenir RF = 3 réplicas → cass1 + cass2 + cass3
```

Ce calcul est **déterministe** : n'importe quel nœud (et le driver client, *token-aware*) sait retrouver les réplicas d'une station sans consulter d'annuaire central. Le nœud qui reçoit la requête du client devient **coordinateur** : il la transmet aux réplicas concernés et attend le nombre de réponses exigé par le niveau de cohérence.

---

## 5. Niveaux de cohérence avec 3 nœuds UP

📸 Capture 3 : `docs/captures/03_tests_coherence.png` · Preuves : `05_coherence_3_noeuds.txt` et `05_coherence_3_noeuds_trace.txt`

```sql
CONSISTENCY ONE;    SELECT * FROM velib_cluster.stations_velib WHERE station_id = '<id>';
CONSISTENCY QUORUM; SELECT * FROM velib_cluster.stations_velib WHERE station_id = '<id>';
CONSISTENCY ALL;    SELECT * FROM velib_cluster.stations_velib WHERE station_id = '<id>';
```

Pour la station `15047` (« Morillons - Dantzig »), `ONE`, `QUORUM` et `ALL` ont toutes renvoyé la même ligne : capacité 52, 1 vélo disponible et 51 bornes disponibles.

### Question 6 — Comparaison

| Niveau | Réplicas nécessaires (RF = 3) | Garantie | Disponibilité | Pannes tolérées |
|---|---:|---|---|---:|
| `ONE` | 1 | Faible : on lit la copie du premier réplica qui répond, elle peut être en retard (cohérence à terme) | Maximale, latence minimale | 2 nœuds |
| `QUORUM` | 2 (⌊3/2⌋ + 1) | Forte si lectures **et** écritures sont en QUORUM : R + W = 2 + 2 > 3, les deux majorités se recouvrent forcément sur au moins un réplica à jour | Bon compromis | 1 nœud |
| `ALL` | 3 | Maximale : tous les réplicas ont répondu | La plus faible, la latence est celle du réplica le plus lent | 0 |

- **ONE** : le coordinateur renvoie la réponse dès qu'un réplica a répondu. Si la dernière écriture n'a atteint qu'un autre réplica, la lecture peut renvoyer une valeur ancienne. Tant qu'un seul réplica est vivant, ça fonctionne.
- **QUORUM** : le coordinateur attend 2 réponses et renvoie la plus récente (horodatage d'écriture). En cas de divergence entre les deux, il déclenche un *read repair*. Il supporte la perte d'un nœud.
- **ALL** : il faut les 3 réponses. La moindre indisponibilité fait échouer la requête : c'est le niveau le plus cohérent, mais aussi celui qui sacrifie la disponibilité.

Avec les 3 nœuds `UN`, les trois lectures réussissent et renvoient la même ligne. Les traces confirment les lectures sur les réplicas; le contrôle de panne ci-dessous montre ensuite que `ALL` échoue lorsque cass3 est indisponible.

Lien avec CAP : Cassandra est un système **AP** dont la cohérence est **réglable requête par requête** ; le niveau de cohérence est précisément le curseur entre C et A.

---

## 6. Simulation de panne

📸 Capture 4 : `docs/captures/04_panne_cass3.png` · Preuve : `docs/preuves/06_panne_cass3.txt`

```bash
docker stop cass3
docker exec cass1 nodetool status
```

```text
Datacenter: dc1
DN  172.18.0.4  163.65 KiB  16  100.0%  1c3b0638-d0c9-4b05-9a3f-01a3676a6e64  rack3
UN  172.18.0.3  158.13 KiB  16  100.0%  d248f927-cc84-49ea-a083-0bd8834e0477  rack2
UN  172.18.0.2  147.12 KiB  16  100.0%  1d1e5965-48c9-4b3d-9f4c-caa857d686a2  rack1
```

### Question 7

- `cass1` et `cass2` restent en **`UN`**.
- `cass3` passe en **`DN`** (*Down / Normal*) : il est injoignable, mais il est toujours membre du cluster et **propriétaire de ses tokens**. Cassandra le considère comme temporairement absent, pas comme retiré (il n'y a ni décommission ni redistribution des données).
- **2 nœuds sur 3 sont disponibles** ; pour chaque station, 2 réplicas sur 3 sont donc joignables.

La panne est détectée par le **gossip** et le *failure detector* (phi accrual) : avec un `docker stop`, le nœud annonce son arrêt et l'état passe en `DN` presque immédiatement ; une panne brutale (crash, coupure réseau) serait détectée au bout de quelques secondes.

---

## 7. Lectures pendant la panne

📸 Capture 5 : `docs/captures/05_resultats_panne.png` · Preuves : `07_lectures_pendant_panne.txt` et `07_lectures_pendant_panne_trace.txt`

Pour `station_id = '15047'`, `ONE` et `QUORUM` ont renvoyé la même ligne qu'avant la panne (Morillons - Dantzig, capacité 52, 1 vélo et 51 bornes disponibles). `ALL` a échoué avec `Unavailable` : `required_replicas: 3, alive_replicas: 2`.

| Niveau | Réplicas exigés | Réplicas vivants | Résultat |
|---|---:|---:|---|
| `ONE` | 1 | 2 | ✅ OK |
| `QUORUM` | 2 | 2 | ✅ OK |
| `ALL` | 3 | 2 | ❌ `Unavailable` : *Cannot achieve consistency level ALL* (`required_replicas: 3, alive_replicas: 2`) |

Test complémentaire en écriture :

| Écriture | Résultat |
|---|---|
| `INSERT` station `99998` en `ALL` | ❌ `Unavailable`, rien n'est écrit |
| `INSERT` station `99999` en `QUORUM` | ✅ l'écriture réussit et la lecture en `QUORUM` retourne la station. Lors du contrôle, `nodetool listpendinghints` indique qu'aucun hint n'est en attente. |

### Question 8 — Analyse

La règle est la suivante : **une requête réussit si `réplicas vivants ≥ réplicas exigés par le CL`**, et le nombre de réplicas vivants dépend de RF et du nombre de nœuds UP.

```text
RF = 3 réplicas par station
 − 1 nœud en panne (cass3)
 = 2 réplicas vivants
      ONE    : 2 ≥ 1 → OK
      QUORUM : 2 ≥ 2 → OK
      ALL    : 2 < 3 → échec
```

- `ONE` et `QUORUM` fonctionnent parce que **RF = 3 laisse 2 copies intactes** de chaque station sur cass1 et cass2.
- `ALL` échoue **immédiatement** avec une `UnavailableException` et non un timeout : grâce au gossip, le coordinateur sait déjà que cass3 est `DN` et refuse la requête sans même l'envoyer, puisqu'elle ne pourrait pas être satisfaite.
- **Rôle de RF = 3 :** c'est lui qui crée la marge. Avec RF = 1, la perte de cass3 rendrait inaccessibles toutes les stations dont il est le seul propriétaire, quel que soit le CL. Avec RF = 2, `QUORUM` (= 2) échouerait pour les partitions dont un réplica est sur cass3. RF = 3 est la valeur minimale qui permet à `QUORUM` de survivre à la perte d'un nœud, d'où son usage très répandu en production.

---

## 8. Redémarrage de cass3

📸 Capture 6 : `docs/captures/06_retour_cass3.png` · Preuve : `docs/preuves/08_retour_cass3.txt`

```bash
docker start cass3
docker exec cass1 nodetool status
```

Après `docker start cass3`, la première vérification montre encore cass3 en `DN`, puis les vérifications suivantes montrent les trois nœuds en `UN`. L'Host ID de cass3 reste `1c3b0638-d0c9-4b05-9a3f-01a3676a6e64` et son rack reste `rack3`.

### Question 9

- Le nœud **revient dans le cluster** : il reste `DN` pendant le démarrage de la JVM (quelques dizaines de secondes), puis repasse en **`UN`**.
- Il revient **avec la même identité** (même *Host ID*, mêmes tokens, même rack) car ses données et sa configuration sont persistées dans le volume `cass3_data`. Il ne passe donc pas par la phase `UJ` (bootstrap), qui ne concerne qu'un nœud nouveau. Si Docker lui attribue une autre IP, Cassandra fait le lien grâce au Host ID.
- La preuve de panne indique `This node does not have any pending hints` lors de l'appel à `nodetool listpendinghints`. Après le retour, la lecture finale sur cass3 retrouve bien la station `99999`, mais les sorties conservées ne permettent pas d'attribuer cette resynchronisation avec certitude au *hinted handoff*.

---

## 9. Vérification finale

📸 Capture 7 : `docs/captures/07_verification_finale.png` · Preuves : `09_verification_finale.txt` et `09_verification_finale_trace.txt`

Après le retour de cass3, les lectures `ONE`, `QUORUM` et `ALL` sur la station `15047` réussissent. La lecture en `ONE` exécutée depuis cass3 retourne aussi `99999` (`Station Test Panne`, capacité 30, 15 vélos et 15 bornes). Enfin, `SELECT count(*)` en `ALL` depuis cass3 retourne **101** lignes : les 100 stations importées et la station de test `99999`.

### Question 10 — Observations après le retour de cass3

```text
Donnée                 l'état d'une station Vélib' (ex. station '<id>')
   ↓
Partition              station_id → hash Murmur3 → token → plage d'un nœud
   ↓
Réplication            RF = 3 → une copie sur cass1, cass2 et cass3
   ↓
Panne d'un nœud        cass3 DN → il reste 2 copies sur 2 nœuds UP
   ↓
Données toujours       ONE et QUORUM répondent ; seul ALL échoue.
accessibles            Les écritures en QUORUM continuent ; aucun hint en attente
                       n'est signalé lors du contrôle
   ↓
Retour du nœud         cass3 repasse UN ; la station 99999 est lisible sur cass3
                       et ALL refonctionne (le mécanisme de resynchronisation
                       n'est pas établi par les preuves enregistrées)
```

Après le retour de cass3, **toutes les lectures fonctionnent à nouveau, y compris en `ALL`**. La station `99999`, écrite pendant l'arrêt de cass3, est lisible en `ONE` depuis cass3; la trace montre que cass3 (`172.18.0.4`) exécute la requête et lit une ligne. Le `count(*)` final en `ALL` vaut 101. Les preuves établissent donc que la donnée est disponible après le retour, mais pas quel mécanisme précis l'a resynchronisée.

Plusieurs mécanismes Cassandra peuvent contribuer à la resynchronisation, mais les sorties enregistrées ne permettent pas d'identifier lequel a rendu `99999` disponible sur cass3 :

1. **Hinted handoff :** Cassandra peut conserver puis rejouer les écritures destinées à un nœud `DN`. Dans cette exécution, `nodetool listpendinghints` a indiqué qu'aucun hint n'était en attente au moment du contrôle; cette preuve ne démontre donc pas le mécanisme.
2. **Read repair :** une lecture qui détecte une divergence peut contribuer à corriger les réplicas, selon la requête et la configuration.
3. **Anti-entropy repair (`nodetool repair`) :** il compare les réplicas par arbres de Merkle et constitue un mécanisme de réparation complémentaire. La sortie enregistrée confirme que `repair` a été lancé dans le TP.

---

## 10. Synthèse

```text
3 nœuds (dc1, 3 racks)
   ↓
RF = 3 (NetworkTopologyStrategy)
   ↓
Chaque station Vélib' répliquée sur cass1, cass2, cass3
   ↓
Arrêt de cass3
   ↓
2 nœuds disponibles = 2 réplicas vivants par station
   ↓
ONE ✅  QUORUM ✅  ALL ❌ (Unavailable)
   ↓
Redémarrage de cass3 → DN puis UN, donnée `99999` à nouveau lisible
   ↓
`count(*) = 101` en ALL ; `99999` est lisible sur cass3
```

### Question 11 — Pourquoi la réplication permet-elle de continuer à fonctionner ?

Parce qu'une donnée n'est pas stockée à un seul endroit. Avec RF = 3, chaque station est répliquée sur trois nœuds placés sur trois racks différents : la perte de l'un d'eux laisse deux réplicas joignables. Dans cette exécution, `ONE` et `QUORUM` continuent de répondre, tandis que `ALL` échoue; après le retour du nœud, `ALL` fonctionne à nouveau et `99999` est lisible sur cass3. Les preuves montrent le résultat de cette reprise, mais ne permettent pas d'identifier le mécanisme précis de resynchronisation.

### Conclusion

Ce TP montre concrètement que la tolérance aux pannes de Cassandra repose sur le couple **RF × Consistency Level** :

- **RF** fixe le nombre de copies, donc le nombre de pannes que les données peuvent subir ;
- **CL** fixe combien de copies doivent répondre, donc l'arbitrage entre cohérence et disponibilité.

Avec RF = 3 et des lectures/écritures en `QUORUM`, on obtient à la fois une **cohérence forte** (R + W > RF) et la **tolérance à la perte d'un nœud**, ce qui explique pourquoi c'est la configuration de référence en production. `ALL` n'apporte qu'un gain marginal de cohérence au prix de la disponibilité, et `ONE` maximise la disponibilité et la latence au prix d'une cohérence à terme.

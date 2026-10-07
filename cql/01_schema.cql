-- =============================================================
-- Table métier du TP2 : stations Vélib' (keyspace velib_cluster)
-- Idempotent : ne recrée rien si le keyspace / la table existent.
-- =============================================================

-- Keyspace du TP2 (créé en RF=3 s'il n'existe pas encore)
CREATE KEYSPACE IF NOT EXISTS velib_cluster
WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'dc1': 3
};

-- Si le keyspace existait déjà (ex. RF=1 / SimpleStrategy au TP2),
-- on le passe en NetworkTopologyStrategy avec RF=3 sur dc1.
ALTER KEYSPACE velib_cluster
WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'dc1': 3
};

-- Table métier du TP2 (schéma inchangé)
CREATE TABLE IF NOT EXISTS velib_cluster.stations_velib (
  station_id            text,
  nom_station           text,
  capacite              int,
  vatiques_disponibles  int,
  bornes_disponibles    int,
  derniere_mise_a_jour  timestamp,
  PRIMARY KEY (station_id)
);

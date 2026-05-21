# Plan de Battle Testing — Orus Infra Suite

## Vue d'ensemble

Le plan de test a 5 volets. Certains dépendent de code qui n'existe pas encore (OrusSync), d'autres peuvent commencer immédiatement. Voici la séquence logique :

---

## Phase 0 — Prérequis : Benchmarks baseline

**Priorité absolue.** Sans baseline de performance, les résultats des tests de chaos n'ont aucune référence.

### Fichiers à créer

| Fichier | Objectif |
|---|---|
| `tests/bench/gateway_bench.zig` | Latence p99 et throughput OrusGateway |
| `tests/bench/broker_bench.zig` | Throughput OrusBroker (fsync et batch) |
| `tests/bench/sync_bench.zig` | Durée sync selon conditions réseau |
| `tests/bench/run_all.sh` | Rapport CSV horodaté |

### Critère de passage
Les chiffres cibles du tableau sont atteints sur le matériel cible (laptop + Raspberry Pi 4).

---

## Phase 1 — Tests unitaires + fuzzing (Volet 3)

Le volet le plus avancé — ne dépend que du code existant.

### Fichiers à créer

| Fichier | Objectif |
|---|---|
| `tests/unit/orusgateway/fuzz_parse_field.zig` | Fuzzing de `parseField` |
| `tests/unit/orusgateway/fuzz_tlv.zig` | TLV récursif profond + siblings |
| `tests/unit/orusbroker/fuzz_wal_replay.zig` | WAL corrompus |

### Méthode

```sh
zig build test -Dfuzz=true  # intégré au build system
```

### Critère de passage
Zéro panic sur 10 millions d'itérations.

---

## Phase 2 — Résilience matérielle (Volet 2)

Teste OrusBroker et OrusConnect — déjà implémentés, pas de dépendance bloquante.

### Fichiers à créer

| Fichier | Objectif |
|---|---|
| `tests/battle/kill9_wal.sh` | Injection WAL + `kill -9` broker en milieu d'écriture |
| `tests/battle/slowloris.py` | Simulation Slowloris sur OrusConnect |
| `tests/battle/flood_gateway.zig` | Inondation du BufferPool d'OrusGateway |

### Méthode `kill -9`

```sh
# Lancer le broker en arrière-plan
./orus-broker &
PID=$!

# Injecter 1000 messages
./inject_messages &

# Kill brutal
kill -9 $PID

# Replay
./orus-broker --replay-only
```

### Critère de passage
Les 3 critères du Volet 2 validés par les logs.

---

## Phase 3 — Chaos réseau (Volet 1)

⚠️ **Dépend d'OrusSync** (non encore implémenté). Implémenter OrusSync d'abord.

### Fichiers à créer

| Fichier | Objectif |
|---|---|
| `libs/orussync/` | Composant complet : `vector_clock`, `merkle`, `local_store`, `sync_protocol`, `conflict` |
| `tests/battle/netem_vsat.sh` | Script `tc-netem` avec les 3 scénarios VSAT |
| `tests/battle/netem_split48h.sh` | Script isolation 48h |

### Contrainte matérielle

Nécessite deux machines (ou deux namespaces réseau Linux avec `ip netns`).

**Alternative sans Raspberry Pi pendant le dev :**

```sh
# Créer deux namespaces réseau isolés
ip netns add agence
ip netns add siege

# Connecter avec veth pair
ip link add veth-agence type veth peer name veth-siege
```

---

## Phase 4 — Split-brain financier (Volet 4)

Démonstration de conformité BEAC/BCEAO. Le plus complexe — dépend d'OrusSync complet + d'un Core Banking simulé.

### Fichiers à créer

| Fichier | Objectif |
|---|---|
| `tests/integration/split_brain_demo.sh` | Orchestration des 5 étapes |
| `tests/fixtures/core_banking_mock.zig` | Simulateur Core Banking (répond en ISO 8583) |
| Interface admin | Affichage des `ConflictRecord` |

Ce volet est le **livrable final de la démo DSI** — s'exécute en dernier.

---

## Séquence globale

| Période | Phase | Volet |
|---|---|---|
| Semaine 1–2 | Phase 0 | Benchmarks baseline |
| Semaine 3–4 | Phase 1 | Fuzzing (Volet 3) |
| Semaine 5–6 | Phase 2 | kill -9, Slowloris, flood (Volet 2) |
| Semaine 7–12 | — | Implémenter OrusSync (`libs/orussync/`) |
| Semaine 13–14 | Phase 3 | Chaos réseau (Volet 1) |
| Semaine 15–16 | Phase 4 | Split-brain démo (Volet 4) |
| Semaine 17 | — | Répétition générale sur banc physique Pi 4 + laptop |

---

## Ce qu'on peut commencer immédiatement

Sans attendre l'audit, sans dépendance bloquante :

1. **Benchmarks baseline (Phase 0)** — le code des libs est là
2. **Fuzz tests (Phase 1)** — `parseField`, `tlv`, `wal_replay` sont dans le code existant
3. **Kill-9 WAL (Phase 2)** — OrusBroker existe déjà

# Orus Infra Suite

Infrastructure d'interopérabilité Mobile Money ↔ Banque pour l'Afrique Centrale et Occidentale.

| | |
|---|---|
| **Version** | 0.1.0 — En développement |
| **Langage** | Zig 0.16.0 (version pinnée) |
| **Marché** | Afrique Centrale et Occidentale |
| **Auteur** | Jordy Hierat |

---

## Problème

L'Afrique subsaharienne concentre les plus grands réseaux Mobile Money au monde (Orange Money, MTN MoMo, Wave, Airtel Money), tandis que le système bancaire classique repose sur ISO 8583 — un protocole binaire des années 1980. Ces deux mondes ne communiquent pas nativement.

```
Monde MoMo (moderne)          Monde bancaire (classique)
─────────────────────         ──────────────────────────
REST / JSON / HTTPS           ISO 8583 binaire / TCP raw
Orange Money API              Core Banking Temenos T24
MTN MoMo API                  Core Banking Flexcube
Wave API                      Switch GIMAC / GIM-UEMOA
```

Orus Infra Suite est le pont bidirectionnel entre ces deux mondes.

---

## Flux Bidirectionnel

**Direction 1 — MoMo → Banque**

```
MoMo webhook
    │ POST JSON
    ▼
OrusConnect
(valide, traduit JSON → InternalMessage)
    │ InternalMessage
    ▼
OrusBroker
(persiste, route, exactly-once)
    │ InternalMessage
    ▼
OrusGateway
(traduit InternalMessage → ISO 8583)
    │ ISO 8583
    ▼
Core Banking
```

**Direction 2 — Banque → MoMo**

```
Core Banking
    │ ISO 8583
    ▼
OrusGateway
(traduit ISO 8583 → InternalMessage)
    │ InternalMessage
    ▼
OrusBroker
(persiste, route vers le bon topic)
    │ InternalMessage
    ▼
OrusConnect
(traduit InternalMessage → POST API MoMo)
    │ REST/JSON
    ▼
Orange Money / MTN / Airtel
```

---

## Les 5 Produits

```
┌──────────────────────────────────────────────────────────────────────┐
│                        ORUS INFRA SUITE                              │
│                                                                      │
│  ┌─────────────────────┐         ┌──────────────────────────────┐    │
│  │   OrusConnect      │         │      OrusGateway            │    │
│  │                     │         │                              │    │
│  │  REST/JSON → IM     │         │  IM → ISO 8583               │    │
│  │  Orange Money       │         │  ISO 8583 → IM               │    │
│  │  MTN MoMo           │         │  Dialectes TOML              │    │
│  │  Wave / Airtel      │         │  GIMAC / Visa / Mastercard   │    │
│  └──────────┬──────────┘         └──────────────┬───────────────┘    │
│             │                                   │                    │
│             │    InternalMessage (orusshared)   │                    │
│             └──────────────┬────────────────────┘                    │
│                            │                                         │
│             ┌──────────────▼──────────────┐                          │
│             │       OrusBroker            │                          │
│             │                              │                          │
│             │  Exactly-once, WAL, Topics   │                          │
│             │  Déduplication, Partitions   │                          │
│             └──────────────┬───────────────┘                          │
│                            │                                         │
│             ┌──────────────▼──────────────┐                          │
│             │  Core Banking / Switch       │  ← Systèmes du client   │
│             │  GIMAC / GIM-UEMOA / Visa    │                          │
│             └─────────────────────────────┘                          │
│                                                                      │
│  ┌───────────────────────────────────────────────────────────────┐   │
│  │  OrusSync — Agent déployé dans chaque agence physique        │   │
│  │  WAL local + Merkle Tree + Vector Clocks                      │   │
│  │  Synchronise avec OrusBroker siège quand online              │   │
│  └───────────────────────────────────────────────────────────────┘   │
│                                                                      │
│  orusshared — InternalMessage — contrat commun à tous les produits  │
└──────────────────────────────────────────────────────────────────────┘
```

| Produit | Rôle | Interface IN | Interface OUT |
|---|---|---|---|
| **orusshared** | Contrat de données commun. Struct `InternalMessage` partagée par tous. | N/A | N/A |
| **OrusConnect** | Reçoit les requêtes MoMo/REST. Traduit en `InternalMessage`. | REST/JSON HTTPS | InternalMessage |
| **OrusGateway** | Traduit `InternalMessage` en ISO 8583 et vice versa. | InternalMessage | ISO 8583 TCP |
| **OrusBroker** | Persiste, route et livre les messages avec garantie exactly-once. | InternalMessage | InternalMessage |
| **OrusSync** | Stocke les transactions offline dans les agences. Réconcilie au retour. | InternalMessage | InternalMessage |

---

## Proposition de Valeur

| Attribut | Engagement |
|---|---|
| **Binaires** | Chaque composant < 5 MB. Zéro dépendance runtime. Ubuntu 18+, Debian, CentOS 7+. |
| **Mémoire** | Suite complète < 128 MB RAM. Fonctionne sur un Raspberry Pi 4. |
| **Latence** | < 1 ms de traitement pur par transaction (hors réseau). |
| **Nouveau dialecte ISO** | Ajouter un nouveau switch = écrire un fichier TOML. Zéro modification du code. |
| **Nouveau MoMo** | Ajouter un nouvel opérateur = écrire un fichier TOML adapter. Zéro modification du code. |
| **Résilience** | Zéro perte de transaction en cas de crash. WAL fsync. Exactly-once sémantique. |
| **Offline** | Agences opérationnelles pendant les coupures réseau. Sync automatique au retour. |

---

## Structure du Projet

```
orusinfra/
  build.zig              ← orchestre tout le monorepo
  build.zig.zon          ← pin Zig 0.14.0
  .gitignore
  LICENSE
  README.md
  docs/
    CDC_KongoInfraSuite_Unifie.docx
  libs/
    orusshared/         ← contrat commun
      message.zig
      serialize.zig
      hash.zig
      metrics.zig
      errors.zig
    orusconnect/        ← lib (logique pure, sans main)
      http_server.zig
      auth/
      adapters/
      state/
      broker_client.zig
    orusgateway/        ← lib
      schema_loader.zig
      bitmap.zig
      field_parser.zig
      tlv_engine.zig
      parser.zig
      builder.zig
      validator.zig
      transport.zig
      broker_client.zig
    orusbroker/         ← lib
      wal.zig
      dedup.zig
      topics.zig
      protocol.zig
      transport.zig
    orussync/           ← lib
      vector_clock.zig
      merkle.zig
      local_store.zig
      sync_protocol.zig
      conflict.zig


  tests/
    unit/                ← tests unitaires par lib
    integration/         ← tests cross-produits
    battle/              ← scripts battle testing (tc-netem, kill -9)
    fixtures/            ← messages ISO 8583 réels anonymisés
      gimac_samples.bin
      visa_samples.bin
      momo_samples.json
  schemas/               ← fichiers TOML partagés
    iso8583/
      gimac.toml
      visa.toml
      mastercard.toml
    momo/
      orange_money.toml
      mtn_momo.toml
      wave.toml
```

---

## Roadmap d'Implémentation

| Jalon | Semaine | Livrable |
|---|---|---|
| M1 — Contrat commun | 2 | `orusshared` complet avec tests exhaustifs |
| M2 — MVP MoMo | 8 | OrusConnect + Orange Money. Démo possible. |
| M3 — Pont complet | 24 | OrusConnect + OrusGateway. MoMo ↔ Banque opérationnel. |
| M4 — Suite fiable | 41 | + OrusBroker. Exactly-once. Production-ready. |
| M5 — Suite complète | 57 | + OrusSync. Agences offline couvertes. |
| M6 — Battle tested | 65 | Framework de test passé. Prêt pour présentation BEAC/BCEAO. |

---

## Prérequis

- Zig 0.14.0 — version exacte pinnée dans `build.zig.zon`
- Linux (Ubuntu 18+, Debian, CentOS 7+) pour la production
- Raspberry Pi 4 ou supérieur pour les déploiements agence

```sh
# Build de tous les produits
zig build

# Tests unitaires
zig build test

# Build d'un produit spécifique
zig build connect
zig build gateway
zig build broker
zig build sync
```

---

## Sécurité

- PAN masqué dans tous les logs — `476200xxxxxx1234` (PCI-DSS Req 3.4)
- TLS 1.2+ sur toutes les connexions (PCI-DSS Req 4.1)
- mTLS obligatoire pour OrusSync — certificat unique par agence
- WAL avec CRC32 + magic bytes `0xDEADBEEF` — corruption détectée au redémarrage
- Conflits financiers offline : jamais auto-résolus, escalade humaine obligatoire

---

## Glossaire Rapide

| Terme | Définition |
|---|---|
| **ISO 8583** | Protocole bancaire binaire des années 1980. Standard ATM/POS/interbancaire. |
| **GIMAC** | Switch interbancaire Afrique Centrale (zone CEMAC). |
| **GIM-UEMOA** | Switch interbancaire Afrique Occidentale francophone. |
| **InternalMessage** | Struct Zig universelle représentant une transaction dans tout le système. |
| **WAL** | Write-Ahead Log — journal disque garantissant la durabilité en cas de crash. |
| **Exactly-once** | Garantie : un message traité exactement une fois, jamais perdu ni dupliqué. |
| **Vector Clock** | Horloge logique distribuée pour détecter les conflits sans horloge physique. |
| **Merkle Tree** | Arbre de hashes pour comparer deux états distribués en O(log N). |
| **TLV** | Tag-Length-Value — structure des données EMV (champ 55 ISO 8583). |
| **STAN** | System Trace Audit Number — numéro de séquence unique par transaction. |

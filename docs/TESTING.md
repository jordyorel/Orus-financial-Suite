# Orus Suite — Guide de test

Ce guide couvre les tests depuis la compilation jusqu'aux flux bout-en-bout pour MTN et Airtel Congo.

---

## Prérequis

| Outil | Version minimale | Vérification |
|-------|-----------------|--------------|
| Zig   | 0.16.0          | `zig version` |
| curl  | toute           | `curl --version` |
| nc    | toute           | `nc -h 2>&1` |

---

## 1. Compilation

```bash
# Tests et développement local
zig build

# Pilote / remise partenaire — toujours utiliser ReleaseSafe
zig build -Doptimize=ReleaseSafe

# Vérifier que les 3 binaires sont produits
ls zig-out/bin/
# → orusbroker  orusconnect  orusgateway
```

---

## 2. Tests unitaires et d'intégration

```bash
# Tous les tests en une commande
zig build test --summary all

# Résultat attendu : tous les tests passent
# Build Summary: 5/5 steps succeeded; N/N tests passed
```

Les tests couvrent :
- Parsing et sérialisation ISO 8583 (bitmap, LLVAR, LLLVAR)
- Traduction InternalMessage ↔ ISO 8583
- Pipeline Gateway (0200 → 0210)
- Adapters MTN MoMo et Airtel Money (auth, validation, callback)
- OrusBroker (WAL, curseur exactly-once, déduplication)

---

## 3. Configuration de l'environnement

```bash
cp .env.example .env
```

Ouvrir `.env` et remplir au minimum :

```bash
# Tokens sandbox MTN (https://momodeveloper.mtn.com)
MTN_BEARER_TOKEN=<votre_token>
MTN_CALLBACK_TOKEN=<votre_callback_secret>

# Token sandbox Airtel (https://developers.airtel.africa)
AIRTEL_BEARER_TOKEN=<votre_token>

# Secret de hachage PAN — changer avant tout déploiement
PAN_HASH_SEED=un_secret_de_64_bits_en_decimal

# Banque core (pour tests locaux, voir section 6 ci-dessous)
GATEWAY_BANK_HOST=127.0.0.1
GATEWAY_BANK_PORT=5000
```

Pour les tests locaux sans banque réelle, laisser `GATEWAY_BANK_HOST=127.0.0.1` et utiliser le simulateur de la section 6.

---

## 4. Démarrage de la suite

```bash
./scripts/run.sh
```

Le script démarre les services dans l'ordre correct (broker en premier), attend que le broker soit prêt, puis lance gateway et connect. Les logs sont dans `./logs/`.

**Arrêt propre :**
```bash
kill $(cat /tmp/orus.pid)
# ou simplement Ctrl-C dans le terminal où run.sh tourne
```

---

## 5. Health checks

Vérifier que les 3 services répondent avant de lancer des tests fonctionnels :

```bash
curl -sf http://localhost:8080/health && echo "OrusConnect OK"
curl -sf http://localhost:7771/       && echo "OrusBroker  OK"
curl -sf http://localhost:7781/       && echo "OrusGateway OK"
```

Résultat attendu pour chaque commande : `OK` affiché sur la sortie standard.

---

## 6. Simulateur de banque (tests sans banque réelle)

Pour tester sans connexion à un core banking, lancer ce simulateur dans un terminal séparé. Il accepte une connexion ISO 8583 et répond systématiquement avec un 0210 approved (RC=00).

```bash
# Simulateur Python — port 5000
python3 - <<'EOF'
import socket, struct

def handle(conn):
    data = conn.recv(4096)
    if len(data) < 2:
        conn.close()
        return
    # Réponse 0210 minimale : MTI + bitmap vide + field 39 = "00"
    mti = b"0210"
    # Bitmap primaire avec bit 39 activé (field 39 = RC)
    # Bit 39 → octet 4, bit 1 (0-indexed: bit 32+6 = 38 → octet 4, valeur 0x02)
    bitmap = bytes([0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00])
    f39 = b"00"  # approved
    body = mti + bitmap + f39
    length = struct.pack(">H", len(body))
    conn.sendall(length + body)
    conn.close()

srv = socket.socket()
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", 5000))
srv.listen(5)
print("Simulateur banque en écoute sur :5000 (Ctrl-C pour arrêter)")
while True:
    conn, addr = srv.accept()
    print(f"Connexion depuis {addr}")
    handle(conn)
EOF
```

---

## 7. Direction 1 — MoMo → Banque

Ce flux se déroule en deux étapes : la demande initiale, puis la confirmation par callback.

### 7a. MTN MoMo — demande de paiement

```bash
# Étape 1 : envoyer la demande (répond 202 Accepted)
RESPONSE=$(curl -s -X POST http://localhost:8080/collection/v1_0/requesttopay \
  -H "Authorization: Bearer $MTN_BEARER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "msisdn":     "242065001234",
    "amount":     5000,
    "currency":   "XAF",
    "externalId": "test-001"
  }')

echo "$RESPONSE"
# Attendu : {"referenceId":"<hex_32_chars>"}

REF=$(echo "$RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin)['referenceId'])")
echo "referenceId = $REF"
```

```bash
# Étape 2 : simuler le callback MTN (confirmation USSD)
curl -s -X POST http://localhost:8080/webhooks/mtn \
  -H "X-Callback-Token: $MTN_CALLBACK_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"referenceId\":\"$REF\",\"status\":\"SUCCESSFUL\"}"
# Attendu : {}  (HTTP 200)
```

Après le callback, OrusConnect publie le message dans OrusBroker, OrusGateway le consomme et envoie le 0200 à la banque. La réponse 0210 est loguée dans `logs/orusgateway.log`.

**Vérifier dans les logs :**
```bash
grep "processed mti=0210" logs/orusgateway.log
# Attendu : [timestamp] gateway: processed mti=0210 rc=00
```

### 7b. Airtel Money — demande de paiement

```bash
# Étape 1 : envoyer la demande
RESPONSE=$(curl -s -X POST http://localhost:8080/v1/airtel/payment \
  -H "Authorization: Bearer $AIRTEL_BEARER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "msisdn":    "242055009876",
    "amount":    10000,
    "currency":  "XAF",
    "reference": "test-airtel-001"
  }')

echo "$RESPONSE"
# Attendu : {"transactionId":"<hex_32_chars>","status":"PENDING"}

TX=$(echo "$RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin)['transactionId'])")
echo "transactionId = $TX"
```

```bash
# Étape 2 : simuler le callback Airtel
curl -s -X POST http://localhost:8080/webhooks/airtel \
  -H "Authorization: Bearer $AIRTEL_BEARER_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"transactionId\":\"$TX\",\"status\":\"SUCCESS\"}"
# Attendu : {}  (HTTP 200)
```

### 7c. Cas d'erreur Direction 1

```bash
# Authentification manquante → 401
curl -s -o /dev/null -w "%{http_code}" \
  -X POST http://localhost:8080/collection/v1_0/requesttopay \
  -H "Content-Type: application/json" \
  -d '{"msisdn":"242065001234","amount":5000,"currency":"XAF"}'
# Attendu : 401

# Montant manquant → 400
curl -s -o /dev/null -w "%{http_code}" \
  -X POST http://localhost:8080/collection/v1_0/requesttopay \
  -H "Authorization: Bearer $MTN_BEARER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"msisdn":"242065001234","currency":"XAF"}'
# Attendu : 400

# Callback avec status FAILED → entrée supprimée, pas de publication
curl -s -X POST http://localhost:8080/webhooks/mtn \
  -H "X-Callback-Token: $MTN_CALLBACK_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"referenceId":"inexistant","status":"FAILED"}'
# Attendu : 404
```

---

## 8. Direction 2 — Banque → MoMo

Ce flux est déclenché par la banque qui envoie un message ISO 8583 sur le port 7581 d'OrusGateway. OrusGateway traduit, publie sur OrusBroker, et OrusConnect consomme et dispatche vers l'API MoMo.

```bash
# Simuler un message ISO 8583 entrant depuis la banque
# (nécessite un client ISO 8583 ou le script Python ci-dessous)

python3 - <<'EOF'
import socket, struct

# Message 0200 minimal : MTI + bitmap + field 2 (MSISDN) + field 4 (montant) + field 49 (devise)
mti = b"0200"

# Bitmap avec bits 2, 4, 49 activés
# Bit 2 → octet 0, valeur 0x40
# Bit 4 → octet 0, valeur 0x10
# Bit 49 → octet 6 (bits 41-48), bit 49 = premier bit de l'octet 6 = 0x80
bitmap = bytes([0x50, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x00])

# Field 2 : MSISDN 242065001234 (LLVAR, longueur sur 2 digits ASCII)
msisdn = b"242065001234"
f2 = b"%02d" % len(msisdn) + msisdn  # "12242065001234"

# Field 4 : montant 5000 (12 digits fixes)
f4 = b"000000005000"

# Field 49 : devise XAF (3 chars fixes)
f49 = b"XAF"

body = mti + bitmap + f2 + f4 + f49
length_prefix = struct.pack(">H", len(body))

s = socket.socket()
s.connect(("127.0.0.1", 7581))
s.sendall(length_prefix + body)

# Lire la réponse provisoire (0210 RC=00)
resp_len_bytes = s.recv(2)
resp_len = struct.unpack(">H", resp_len_bytes)[0]
resp = s.recv(resp_len)
print(f"Réponse banque : MTI={resp[:4].decode()} RC={resp[-2:].decode()}")
s.close()
EOF
```

**Vérifier dans les logs OrusConnect :**
```bash
grep "dispatch.*MTN\|dispatch.*Airtel" logs/orusconnect.log
# Si le MSISDN commence par 24206 → dispatché vers MTN
# Si le MSISDN commence par 24205 → dispatché vers Airtel
```

---

## 9. Test de supervision (restart automatique)

```bash
# Démarrer la suite
./scripts/run.sh &

# Attendre que tout soit prêt
sleep 3

# Tuer OrusBroker manuellement
pkill -f orusbroker

# Observer les logs — le supervisor doit redémarrer le processus en moins de 2s
tail -f logs/orusbroker.log
# Attendu : [timestamp] [orusbroker] exited (code=...), restarting in 1s
#           [timestamp] [orusbroker] starting
```

---

## 10. Test SIGTERM (arrêt propre)

```bash
# Démarrer la suite en arrière-plan
./scripts/run.sh &
SUITE_PID=$!
sleep 3

# Envoyer SIGTERM au supervisor
kill $SUITE_PID

# Vérifier que tous les services s'arrêtent
sleep 2
pgrep -l orusbroker orusconnect orusgateway && echo "ERREUR: services encore en vie" || echo "OK: tous arrêtés"
```

---

## 11. Résumé des ports

| Service        | Port principal | Health check |
|----------------|---------------|-------------|
| OrusBroker     | 7770 (TCP)    | 7771 (HTTP) |
| OrusGateway D1 | 7780 (TCP)    | 7781 (HTTP) |
| OrusGateway D2 | 7581 (ISO 8583 TCP) | — |
| OrusConnect    | 8080 (HTTP)   | `GET /health` |

---

## 12. Variables d'environnement de référence

Voir [.env.example](.env.example) pour la liste complète avec descriptions.

---

## 13. Ajouter un opérateur MoMo — zéro modification de code

1. Créer un fichier `schemas/momo/<operateur>.toml` en suivant le modèle de `mtn_momo.toml`.
2. Renseigner les variables d'environnement nommées dans la section `[env]` du fichier.
3. Redémarrer OrusConnect — le schéma est chargé automatiquement au démarrage.

```bash
# Exemple : Wave Money (préfixe fictif 24207)
cp schemas/momo/mtn_momo.toml schemas/momo/wave_money.toml
# Éditer wave_money.toml (id, prefixes, chemins HTTP, noms de variables d'env)
# Ajouter WAVE_BEARER_TOKEN=... dans .env
# Redémarrer OrusConnect
```

Par défaut OrusConnect scanne `./schemas/momo`. Pour pointer vers un autre répertoire :

```bash
CONNECT_SCHEMA_DIR=/etc/orus/schemas/momo ./zig-out/bin/orusconnect
```

## 14. Ajouter un dialecte ISO 8583 — zéro modification de code

Pointer `GATEWAY_ISO_SCHEMA_PATH` vers le fichier TOML du nouveau dialecte :

```bash
GATEWAY_ISO_SCHEMA_PATH=/etc/orus/schemas/iso8583/uemoa_gimac_v2.toml ./zig-out/bin/orusgateway
```

Si `GATEWAY_ISO_SCHEMA_PATH` n'est pas défini, le gateway utilise `GATEWAY_ISO_SCHEMA` (valeurs : `gimac`, `visa`, `mastercard`).

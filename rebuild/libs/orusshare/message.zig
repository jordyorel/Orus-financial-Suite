// ── CONCEPT 1 : Importer la bibliothèque standard ───────────────────────────
//
// En Zig, rien n'est implicitement disponible.
// `@import` est une fonction builtin (commence par @) qui charge un module.
// `std` est le module standard fourni avec Zig.
// `const` = valeur qui ne change jamais (comme `const` en JS/C)
const std = @import("std");


// ── CONCEPT 2 : Les enums ────────────────────────────────────────────────────
//
// Un enum = un type qui ne peut avoir qu'une valeur parmi une liste fixe.
// Ici, un message peut venir de 3 origines — jamais d'autre chose.
//
// `pub` = "public" — visible depuis les autres fichiers qui importent ce module
// `const` = ce type ne change pas à l'exécution
//
// En mémoire : Zig stocke un enum en u8 par défaut (1 octet, 256 valeurs max)
// Tu peux forcer : `enum(u16) { ... }` pour 65536 valeurs
pub const MessageOrigin = enum {
    iso8583,    // Venu d'un réseau ISO 8583 (banque/ATM)
    rest_json,  // Venu d'une API REST (MoMo webhook)
    internal,   // Créé en interne par notre système
};


// Même concept — qui a envoyé/reçoit ce message ?
pub const ServiceProvider = enum {
    none,         // Pas de provider (messages internes)
    orange_money, // Orange Money Cameroun
    mtn_momo,     // MTN MoMo
    wave,         // Wave
    airtel_money, // Airtel Money
    custom,       // Provider personnalisé
};


// ── CONCEPT 3 : Les structs ──────────────────────────────────────────────────
//
// Un struct = un groupe de valeurs nommées.
// C'est la structure de données centrale de tout le système.
// Chaque message qui circule entre composants EST un InternalMessage.
pub const InternalMessage = struct {

    // ── Identité ──────────────────────────────────────────────────────────

    // CONCEPT : Tableaux de taille fixe `[N]u8`
    // [16]u8 = exactement 16 octets, alloués sur la stack, jamais heap
    // Pas de pointeur, pas d'allocation = zéro risque de fuite mémoire
    // C'est un UUID (Universally Unique ID) sous forme binaire brute
    msg_id: [16]u8,

    // CONCEPT : Slices `[]const u8`
    // []const u8 = une "slice" de bytes — c'est comme une string en Zig
    // Une slice = { ptr: *u8, len: usize }
    // `const` = on ne peut pas modifier les bytes pointés
    // Attention : une slice ne possède PAS la mémoire — elle pointe vers elle
    schema_id: []const u8,  // ex: "mtn_cm", "orange_bf"

    // Le topic pub/sub — ex: "transactions.inbound" ou "transactions.outbound"
    topic: []const u8,

    // Nos deux enums — stockés en 1 octet chacun
    origin:   MessageOrigin,
    provider: ServiceProvider,

    // ── Contenu ISO 8583 ──────────────────────────────────────────────────

    // MTI = Message Type Indicator — le "verbe" ISO 8583
    // "0100" = demande d'autorisation
    // "0110" = réponse d'autorisation
    // "0200" = demande de paiement
    // [4]u8 parce que c'est toujours exactement 4 chiffres ASCII
    mti: [4]u8,

    // CONCEPT : Slice de struct
    // []FieldEntry = slice de FieldEntry (défini ci-dessous)
    // C'est comme un tableau dynamique, mais la taille est connue à la création
    fields: []FieldEntry,

    // ── Métadonnées de routage ────────────────────────────────────────────

    // STAN = System Trace Audit Number — numéro de séquence de transaction
    // Toujours 6 chiffres ASCII, ex: "000042"
    stan: [6]u8,

    // Hash du PAN (Primary Account Number = numéro de carte)
    // On ne stocke JAMAIS le numéro de carte en clair — seulement son hash
    // u64 = entier non-signé 64 bits
    pan_hash: u64,

    // Le montant en centimes (pas de virgule flottante pour l'argent !)
    // 5000 XAF = 500000 (en centimes de centime, dépend de la devise)
    // i64 = entier signé 64 bits (peut être négatif pour des remboursements)
    amount: i64,

    // Code ISO 4217 de la devise — toujours 3 lettres : "XAF", "USD", "EUR"
    currency: [3]u8,

    // Timestamp en nanosecondes depuis l'époque Unix
    // i64 car std.time.nanoTimestamp() retourne i64
    received_at: i64,

    // ── Traçabilité ───────────────────────────────────────────────────────

    // Adresse IP source — 16 octets pour supporter IPv6
    // (IPv4 prend 4 octets, IPv6 en prend 16)
    source_ip: [16]u8,

    // Combien d'étapes ce message a traversé (MoMo → Broker → Gateway = 2)
    // u8 = 0 à 255, largement suffisant
    hop_count: u8,

    // CONCEPT : Types optionnels avec `?`
    // ?[]const u8 = soit une slice de bytes, soit null
    // En Zig, null n'existe que pour les types optionnels — pas par défaut
    // Si tu déclares `topic: []const u8`, tu NE PEUX PAS mettre null dedans
    external_id: ?[]const u8,


    // ── CONCEPT 4 : Struct imbriqué ───────────────────────────────────────
    //
    // Un struct DANS un struct.
    // FieldEntry représente un champ ISO 8583 (ex: id=2 → numéro de compte)
    pub const FieldEntry = struct {
        id:    u8,           // Numéro du champ (1-128 en ISO 8583 standard)
        value: []const u8,   // Valeur du champ (toujours du texte)
    };


    // ── CONCEPT 5 : Méthodes sur un struct ───────────────────────────────
    //
    // En Zig, une méthode = une fonction déclarée DANS le struct.
    // `self: *const InternalMessage` = pointeur vers this (en lecture seule)
    // `*const` = je pointe vers un InternalMessage, mais je ne peux pas le modifier
    // `[32]u8` = retourne un tableau de 32 octets (= 256 bits = SHA-256)
    pub fn computeHash(self: *const InternalMessage) [32]u8 {
        // Sha256 = algorithme de hachage cryptographique
        // `.{}` = struct anonyme vide (les options par défaut)
        var h = std.crypto.hash.sha2.Sha256.init(.{});

        // `update()` ajoute des données au hash
        // `&self.msg_id` = pointeur vers le tableau msg_id
        // On donne les données brutes (bytes) au hasher
        h.update(&self.msg_id);
        h.update(&self.mti);
        h.update(&self.stan);

        // Le montant est un i64 — on doit le convertir en 8 bytes
        // `var` = variable locale (mutable)
        // `undefined` = non-initialisé (Zig t'oblige à initialiser avant utilisation)
        var amt_buf: [8]u8 = undefined;
        // writeInt : écrit un entier dans un buffer en big-endian
        // .big = big-endian (octet de poids fort en premier, comme réseau TCP)
        std.mem.writeInt(i64, &amt_buf, self.amount, .big);
        h.update(&amt_buf);

        // finalResult() retourne le hash final — exactement [32]u8
        return h.finalResult();
    }
};


// ── CONCEPT 6 : Les tests en Zig ─────────────────────────────────────────────
//
// Les tests vivent DANS le même fichier que le code (pas de dossier tests/ séparé)
// `zig build test` les trouve et les exécute automatiquement
// `try` = si cette expression retourne une erreur, propage l'erreur vers le caller

test "computeHash: même message = même hash" {
    // Créer un InternalMessage avec tous les champs initialisés
    // CONCEPT : Initialisation de struct avec `.{ .champ = valeur }`
    const msg = InternalMessage{
        // [_]u8{1} ** 16 = tableau de 16 fois la valeur 1
        // `[_]` = Zig infère la taille automatiquement
        // `** N` = répète N fois
        .msg_id    = [_]u8{1} ** 16,
        .schema_id = "gimac",
        .topic     = "transactions.financial",
        .origin    = .rest_json,
        .provider  = .orange_money,

        // "0200".* = transforme le string literal en [4]u8
        // `.*` = dereference — "0200" est un *const [4:0]u8, `.*` le déréférence en [4]u8
        .mti       = "0200".*,

        // `&.{}` = pointeur vers un slice vide — aucun champ pour ce test
        .fields    = &.{},

        .stan        = "123456".*,
        .pan_hash    = 0xdeadbeef,  // 0x = notation hexadécimale
        .amount      = 5_000_000,   // `_` = séparateur visuel (5 millions de centimes)
        .currency    = "XAF".*,
        .received_at = 1_700_000_000_000_000_000,
        .source_ip   = [_]u8{0} ** 16,
        .hop_count   = 1,
        .external_id = null,        // Le champ optionnel est absent
    };

    const h1 = msg.computeHash();
    const h2 = msg.computeHash();

    // expectEqual vérifie que les deux valeurs sont identiques
    // Si elles diffèrent, le test échoue avec un message d'erreur clair
    try std.testing.expectEqual(h1, h2);
}

test "computeHash: montant modifié = hash différent" {
    // `var` au lieu de `const` car on va modifier msg.amount
    var msg = InternalMessage{
        .msg_id    = [_]u8{1} ** 16,
        .schema_id = "gimac",
        .topic     = "transactions.financial",
        .origin    = .rest_json,
        .provider  = .orange_money,
        .mti       = "0200".*,
        .fields    = &.{},
        .stan      = "123456".*,
        .pan_hash  = 0xdeadbeef,
        .amount    = 5_000_000,
        .currency  = "XAF".*,
        .received_at = 1_700_000_000_000_000_000,
        .source_ip = [_]u8{0} ** 16,
        .hop_count = 1,
        .external_id = null,
    };

    const h1 = msg.computeHash();

    // On modifie le montant — le hash doit changer
    msg.amount = 9_999_999;

    const h2 = msg.computeHash();

    // `std.mem.eql` compare deux slices byte par byte
    // `&h1` et `&h2` = on passe les tableaux comme slices
    // `!` = NOT — on vérifie que les hashs sont DIFFÉRENTS
    try std.testing.expect(!std.mem.eql(u8, &h1, &h2));
}

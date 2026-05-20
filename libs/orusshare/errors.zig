// Unified error taxonomy for Orus Infra Suite.
// Convention: prefix + level + sequential number (e.g. C1001, E2003, B3001).
// Every error has a deterministic response. No silent errors.

// ── OrusConnect errors ────────────────────────────────────────────────────────

pub const ConnectError = error{
    // Protocol (C1xxx)
    InvalidJson,          // C1001 — malformed JSON payload
    MissingField,         // C1002 — required field absent
    InvalidSignature,     // C1003 — HMAC or OAuth token invalid
    UnknownProvider,      // C1004 — provider not configured

    // Business (C2xxx)
    AmountOutOfRange,     // C2001 — negative or zero amount
    InvalidCurrency,      // C2002 — unknown ISO 4217 currency code
    DuplicateExternalId,  // C2003 — external_id already processed (idempotence)

    // System (C3xxx)
    BrokerUnavailable,    // C3001 — OrusBroker unreachable
    CallbackTimeout,      // C3002 — no MoMo callback within deadline
};

// ── OrusGateway errors ────────────────────────────────────────────────────────

pub const GatewayError = error{
    // L1 Structural (E1xxx)
    InvalidMti,           // E1001 — MTI not 4 ASCII digits
    BitmapTruncated,      // E1002 — primary or secondary bitmap incomplete
    FieldOverflow,        // E1003 — announced length exceeds buffer
    InvalidBcd,           // E1004 — BCD nibble out of range (> 9)
    TlvMaxDepthExceeded,  // E1005 — TLV recursion depth exceeded
    MessageTooLarge,      // E1006 — message exceeds 4096 bytes
    InvalidHeaderLength,  // E1007 — transport header length mismatch
    UnknownFieldType,     // E1008 — field type not in schema

    // L2 Semantic (E2xxx)
    InvalidPan,           // E2001 — PAN fails Luhn check
    NegativeAmount,       // E2002 — amount field <= 0
    CardExpired,          // E2003 — expiry date in the past
    DuplicateStan,        // E2004 — STAN already seen in dedup window
    MandatoryFieldAbsent, // E2005 — required field not present per schema
    InvalidCurrencyCode,  // E2006 — currency not ISO 4217
    InvalidDateFormat,    // E2007 — date/time field malformed
    SchemaNotFound,       // E2008 — schema_id not loaded

    // L3 Transport (E3xxx)
    UpstreamTimeout,      // E3001 — switch/core banking did not respond
    ConnectionLost,       // E3002 — TCP connection dropped mid-transaction
    ConnectionPoolExhausted, // E3003 — all upstream connections in use
    TlsHandshakeFailed,   // E3004 — TLS negotiation failed
    BrokerUnavailable,    // E3005 — OrusBroker unreachable
};

// ── OrusBroker errors ─────────────────────────────────────────────────────────

pub const BrokerError = error{
    // Protocol (B1xxx)
    MalformedFrame,       // B1001 — frame header invalid
    UnknownFrameType,     // B1002 — frame type byte not recognized
    InvalidMsgId,         // B1003 — msg_id field wrong length or zero
    InvalidTopic,         // B1004 — topic name empty or too long
    FrameTooLarge,        // B1005 — frame exceeds max allowed size

    // Business (B2xxx)
    TopicFull,            // B2001 — topic at capacity (backpressure)
    UnknownTopic,         // B2002 — topic not in allowed list
    ConsumerAlreadyActive, // B2003 — consumer group already subscribed
    DedupWindowCorrupted, // B2004 — dedup ring buffer state inconsistent

    // System (B3xxx)
    WalWriteFailed,       // B3001 — write to WAL returned error
    FsyncFailed,          // B3002 — fsync() returned error
    ConnectionPoolExhausted, // B3003 — no free connection slots
    PartialWrite,         // B3004 — wrote fewer bytes than expected
    SegmentCorrupted,     // B3005 — magic bytes / CRC32 mismatch on replay
};

// ── OrusSync errors ───────────────────────────────────────────────────────────

pub const SyncError = error{
    // Protocol sync (S1xxx)
    InvalidSyncFrame,     // S1001 — sync frame malformed
    SyncTimeout,          // S1002 — peer did not respond within deadline
    PeerRejected,         // S1003 — peer rejected handshake (e.g. expired cert)
    InvalidRootHash,      // S1004 — root hash field wrong length
    MerkleDepthExceeded,  // S1005 — Merkle diff recursion too deep

    // Reconciliation (S2xxx)
    ConflictDetected,     // S2001 — vector clocks show concurrent writes
    VectorClockCorrupted, // S2002 — VC entries inconsistent
    ConflictTableFull,    // S2003 — no room for new ConflictRecord
    InvalidVectorClock,   // S2004 — VC counter decreased (injection attempt)
    ApplyFailed,          // S2005 — could not apply remote operation locally

    // Storage (S3xxx)
    WalFull,              // S3001 — local WAL has reached capacity
    MerkleCorrupted,      // S3002 — Merkle tree internal state inconsistent
    StoreLocked,          // S3003 — local store mutex held by crashed thread
    DiskFull,             // S3004 — filesystem reports no space
};

// ── Shared serialize/deserialize errors ───────────────────────────────────────

pub const SerializeError = @import("serialize.zig").SerializeError;
pub const DeserializeError = @import("serialize.zig").DeserializeError;

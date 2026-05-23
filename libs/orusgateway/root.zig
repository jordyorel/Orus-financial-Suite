const parser = @import("iso8583/parser.zig");

// Pull sub-module tests into the test binary.
comptime {
    _ = @import("iso8583/encoding.zig");
    _ = @import("iso8583/tlv.zig");
    _ = @import("iso8583/schema.zig");
    _ = @import("iso8583/bitmap.zig");
    _ = @import("iso8583/parser.zig");
    _ = @import("translator.zig");
    _ = @import("bank_client.zig");
    _ = @import("gateway.zig");
    _ = @import("broker_client.zig");
    _ = @import("bank_server.zig");
    _ = @import("reconciliation.zig");
}

pub const iso8583 = struct {
    pub const Bitmap   = @import("iso8583/bitmap.zig").Bitmap;
    pub const FieldKind = @import("iso8583/fields.zig").FieldKind;
    pub const FieldSpec = @import("iso8583/fields.zig").FieldSpec;
    pub const SPECS     = @import("iso8583/fields.zig").SPECS;
    pub const IsoMessage = parser.IsoMessage;
    pub const ParseError = parser.ParseError;
    pub const parse      = parser.parse;
    pub const serialize  = parser.serialize;
    pub const parseWithSchema    = parser.parseWithSchema;
    pub const serializeWithSchema = parser.serializeWithSchema;
};

pub const encoding = @import("iso8583/encoding.zig");
pub const tlv      = @import("iso8583/tlv.zig");
pub const schema   = @import("iso8583/schema.zig");

pub const IsoSchema      = schema.IsoSchema;
pub const FieldDef       = schema.FieldDef;
pub const FieldType      = schema.FieldType;
pub const Encoding       = schema.Encoding;
pub const HeaderType     = schema.HeaderType;
pub const DEFAULT_SCHEMA = schema.DEFAULT_SCHEMA;
pub const parseIsoSchema = schema.parseIsoSchema;

pub const translator      = @import("translator.zig");
pub const fromInternal    = translator.fromInternal;
pub const toInternal      = translator.toInternal;
pub const buildReversal   = translator.buildReversal;
pub const isReversal      = translator.isReversal;
pub const isReconciliation = translator.isReconciliation;

pub const reconciliation      = @import("reconciliation.zig");
pub const ReconciliationState = reconciliation.ReconciliationState;

pub const bank_client = @import("bank_client.zig");
pub const BankClient  = bank_client.BankClient;

pub const gateway = @import("gateway.zig");
pub const Gateway = gateway.Gateway;

pub const broker_client = @import("broker_client.zig");
pub const GatewayBrokerClient = broker_client.BrokerClient;

pub const bank_server = @import("bank_server.zig");
pub const BankServer = bank_server.BankServer;

const std = @import("std");

pub const FieldType  = enum { FIXED, LLVAR, LLLVAR, TLV };
pub const Encoding   = enum { ASCII, BCD, BINARY, EBCDIC };
pub const HeaderType = enum { none, length_2, length_4 };

pub const FieldDef = struct {
    id:         u8,
    name:       []const u8,
    field_type: FieldType,
    encoding:   Encoding,
    // Logical length: decimal digit count for BCD/ASCII, byte count for BINARY.
    length:     u16,
    subschema:  ?[]const u8 = null,
};

pub const IsoSchema = struct {
    id:               []const u8,
    version:          []const u8,
    header_type:      HeaderType,
    default_encoding: Encoding,
    mti_encoding:     Encoding,
    fields:           []const FieldDef,
    alloc:            std.mem.Allocator,

    pub fn deinit(self: *IsoSchema) void {
        self.alloc.free(self.fields);
    }

    pub fn getField(self: *const IsoSchema, id: u8) ?*const FieldDef {
        for (self.fields) |*f| if (f.id == id) return f;
        return null;
    }
};

pub const SchemaError = error{ InvalidFormat, MissingField, OutOfMemory };

// Parse an ISO 8583 TOML schema from `src`.
// String values point into `src` (zero-copy). `fields` slice is alloc-owned.
pub fn parseIsoSchema(src: []const u8, alloc: std.mem.Allocator) SchemaError!IsoSchema {
    var schema = IsoSchema{
        .id               = "",
        .version          = "1.0",
        .header_type      = .none,
        .default_encoding = .ASCII,
        .mti_encoding     = .ASCII,
        .fields           = &.{},
        .alloc            = alloc,
    };

    var field_defs: std.ArrayList(FieldDef) = .empty;
    errdefer field_defs.deinit(alloc);

    var current: ?FieldDef = null;
    var section: []const u8 = "";

    var lines = std.mem.splitScalar(u8, src, '\n');
    while (lines.next()) |raw| {
        const line = std.mem.trim(u8, raw, " \t\r");
        if (line.len == 0 or line[0] == '#') continue;

        if (std.mem.eql(u8, line, "[[fields]]")) {
            if (current) |f| field_defs.append(alloc, f) catch return error.OutOfMemory;
            current = .{ .id = 0, .name = "", .field_type = .FIXED, .encoding = .ASCII, .length = 0 };
            section = "fields";
            continue;
        }

        if (line[0] == '[' and line[line.len - 1] == ']') {
            if (current) |f| {
                field_defs.append(alloc, f) catch return error.OutOfMemory;
                current = null;
            }
            section = line[1 .. line.len - 1];
            continue;
        }

        const eq = std.mem.indexOfScalar(u8, line, '=') orelse continue;
        const key = std.mem.trim(u8, line[0..eq], " \t");
        const raw_val = std.mem.trim(u8, line[eq + 1 ..], " \t");
        const val = unquote(raw_val);

        if (std.mem.eql(u8, section, "fields")) {
            if (current) |*f| {
                if (std.mem.eql(u8, key, "id"))       f.id = std.fmt.parseInt(u8,  val, 10) catch return error.InvalidFormat;
                if (std.mem.eql(u8, key, "name"))      f.name = val;
                if (std.mem.eql(u8, key, "type"))      f.field_type = parseFieldType(val)  orelse return error.InvalidFormat;
                if (std.mem.eql(u8, key, "encoding"))  f.encoding   = parseEncoding(val)   orelse return error.InvalidFormat;
                if (std.mem.eql(u8, key, "length"))    f.length = std.fmt.parseInt(u16, val, 10) catch return error.InvalidFormat;
                if (std.mem.eql(u8, key, "subschema")) f.subschema = val;
            }
        } else if (std.mem.eql(u8, section, "meta")) {
            if (std.mem.eql(u8, key, "id"))      schema.id      = val;
            if (std.mem.eql(u8, key, "version")) schema.version = val;
        } else if (std.mem.eql(u8, section, "transport")) {
            if (std.mem.eql(u8, key, "header_type")) schema.header_type = parseHeaderType(val) orelse return error.InvalidFormat;
        } else if (std.mem.eql(u8, section, "encoding")) {
            if (std.mem.eql(u8, key, "default")) schema.default_encoding = parseEncoding(val) orelse return error.InvalidFormat;
            if (std.mem.eql(u8, key, "mti"))     schema.mti_encoding     = parseEncoding(val) orelse return error.InvalidFormat;
        }
    }

    if (current) |f| field_defs.append(alloc, f) catch return error.OutOfMemory;
    if (schema.id.len == 0) return error.MissingField;

    schema.fields = field_defs.toOwnedSlice(alloc) catch return error.OutOfMemory;
    return schema;
}

fn unquote(s: []const u8) []const u8 {
    if (s.len >= 2 and s[0] == '"' and s[s.len - 1] == '"') return s[1 .. s.len - 1];
    return s;
}

fn parseFieldType(s: []const u8) ?FieldType {
    if (std.mem.eql(u8, s, "FIXED"))  return .FIXED;
    if (std.mem.eql(u8, s, "LLVAR"))  return .LLVAR;
    if (std.mem.eql(u8, s, "LLLVAR")) return .LLLVAR;
    if (std.mem.eql(u8, s, "TLV"))    return .TLV;
    return null;
}

fn parseEncoding(s: []const u8) ?Encoding {
    if (std.mem.eql(u8, s, "ASCII"))  return .ASCII;
    if (std.mem.eql(u8, s, "BCD"))    return .BCD;
    if (std.mem.eql(u8, s, "BINARY")) return .BINARY;
    if (std.mem.eql(u8, s, "EBCDIC")) return .EBCDIC;
    return null;
}

fn parseHeaderType(s: []const u8) ?HeaderType {
    if (std.mem.eql(u8, s, "none"))     return .none;
    if (std.mem.eql(u8, s, "length_2")) return .length_2;
    if (std.mem.eql(u8, s, "length_4")) return .length_4;
    return null;
}

// ── Default schema (ASCII-only, mirrors existing hardcoded SPECS) ──────────────

const DEFAULT_FIELDS = [_]FieldDef{
    .{ .id = 2,   .name = "PAN",        .field_type = .LLVAR,  .encoding = .ASCII, .length = 19 },
    .{ .id = 3,   .name = "ProcCode",   .field_type = .FIXED,  .encoding = .ASCII, .length = 6  },
    .{ .id = 4,   .name = "Amount",     .field_type = .FIXED,  .encoding = .ASCII, .length = 12 },
    .{ .id = 5,   .name = "SettlAmt",   .field_type = .FIXED,  .encoding = .ASCII, .length = 12 },
    .{ .id = 7,   .name = "TransDT",    .field_type = .FIXED,  .encoding = .ASCII, .length = 10 },
    .{ .id = 11,  .name = "STAN",       .field_type = .FIXED,  .encoding = .ASCII, .length = 6  },
    .{ .id = 12,  .name = "LocalTime",  .field_type = .FIXED,  .encoding = .ASCII, .length = 6  },
    .{ .id = 13,  .name = "LocalDate",  .field_type = .FIXED,  .encoding = .ASCII, .length = 4  },
    .{ .id = 14,  .name = "ExpDate",    .field_type = .FIXED,  .encoding = .ASCII, .length = 4  },
    .{ .id = 22,  .name = "POSMode",    .field_type = .FIXED,  .encoding = .ASCII, .length = 3  },
    .{ .id = 32,  .name = "AcqInstID",  .field_type = .LLVAR,  .encoding = .ASCII, .length = 11 },
    .{ .id = 37,  .name = "RRN",        .field_type = .FIXED,  .encoding = .ASCII, .length = 12 },
    .{ .id = 38,  .name = "AuthCode",   .field_type = .FIXED,  .encoding = .ASCII, .length = 6  },
    .{ .id = 39,  .name = "RespCode",   .field_type = .FIXED,  .encoding = .ASCII, .length = 2  },
    .{ .id = 41,  .name = "TermID",     .field_type = .FIXED,  .encoding = .ASCII, .length = 8  },
    .{ .id = 42,  .name = "MerchID",    .field_type = .FIXED,  .encoding = .ASCII, .length = 15 },
    .{ .id = 43,  .name = "MerchName",  .field_type = .FIXED,  .encoding = .ASCII, .length = 40 },
    .{ .id = 49,  .name = "Currency",        .field_type = .FIXED,  .encoding = .ASCII,  .length = 3   },
    .{ .id = 60,  .name = "Reconciliation",  .field_type = .LLLVAR, .encoding = .ASCII,  .length = 120 },
    .{ .id = 64,  .name = "MAC",             .field_type = .FIXED,  .encoding = .BINARY, .length = 8   },
    .{ .id = 70,  .name = "NMI",             .field_type = .FIXED,  .encoding = .ASCII,  .length = 3   },
    .{ .id = 96,  .name = "KeyMaterial",     .field_type = .LLVAR,  .encoding = .ASCII,  .length = 32  },
    .{ .id = 102, .name = "AccountID1",      .field_type = .LLVAR,  .encoding = .ASCII,  .length = 28  },
    .{ .id = 103, .name = "AccountID2",      .field_type = .LLVAR,  .encoding = .ASCII,  .length = 28  },
    .{ .id = 128, .name = "MAC2",            .field_type = .FIXED,  .encoding = .BINARY, .length = 8   },
};

pub const DEFAULT_SCHEMA = IsoSchema{
    .id               = "default",
    .version          = "1.0",
    .header_type      = .none,
    .default_encoding = .ASCII,
    .mti_encoding     = .ASCII,
    .fields           = &DEFAULT_FIELDS,
    .alloc            = undefined, // static — never freed
};

// ── Tests ─────────────────────────────────────────────────────────────────────

test "parseIsoSchema: basic fields" {
    const alloc = std.testing.allocator;
    const src =
        \\[meta]
        \\id = "gimac"
        \\version = "2.1"
        \\
        \\[transport]
        \\header_type = "length_2"
        \\
        \\[encoding]
        \\default = "ASCII"
        \\mti = "ASCII"
        \\
        \\[[fields]]
        \\id = 4
        \\name = "Amount"
        \\type = "FIXED"
        \\encoding = "BCD"
        \\length = 12
        \\
        \\[[fields]]
        \\id = 2
        \\name = "PAN"
        \\type = "LLVAR"
        \\encoding = "BCD"
        \\length = 19
    ;
    var schema = try parseIsoSchema(src, alloc);
    defer schema.deinit();

    try std.testing.expectEqualStrings("gimac", schema.id);
    try std.testing.expectEqual(HeaderType.length_2, schema.header_type);
    try std.testing.expectEqual(@as(usize, 2), schema.fields.len);

    const f4 = schema.getField(4).?;
    try std.testing.expectEqual(FieldType.FIXED, f4.field_type);
    try std.testing.expectEqual(Encoding.BCD, f4.encoding);
    try std.testing.expectEqual(@as(u16, 12), f4.length);

    const f2 = schema.getField(2).?;
    try std.testing.expectEqual(FieldType.LLVAR, f2.field_type);
}

test "parseIsoSchema: missing id returns error" {
    const alloc = std.testing.allocator;
    const src = "[meta]\nversion = \"1.0\"\n";
    const result = parseIsoSchema(src, alloc);
    try std.testing.expectError(error.MissingField, result);
}

test "DEFAULT_SCHEMA: getField" {
    const f4 = DEFAULT_SCHEMA.getField(4).?;
    try std.testing.expectEqual(Encoding.ASCII, f4.encoding);
    try std.testing.expectEqual(@as(u16, 12), f4.length);
    try std.testing.expect(DEFAULT_SCHEMA.getField(99) == null);
}

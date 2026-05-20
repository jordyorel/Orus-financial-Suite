// ISO 8583 field specifications for MoMo-bank interop (West/Central Africa).
// kind=fixed → len is the exact byte count on the wire.
// kind=llvar  → 2-digit ASCII length prefix, len is the max data length.
// kind=lllvar → 3-digit ASCII length prefix, len is the max data length.

pub const FieldKind = enum { fixed, llvar, lllvar };

pub const FieldSpec = struct {
    kind: FieldKind,
    len: u16,
};

pub const SPECS: [129]?FieldSpec = buildSpecs();

fn buildSpecs() [129]?FieldSpec {
    var s: [129]?FieldSpec = [_]?FieldSpec{null} ** 129;

    // Field 1: secondary bitmap — handled by parser, not a data field
    s[1] = null;

    s[2]  = .{ .kind = .llvar,  .len = 19  }; // PAN
    s[3]  = .{ .kind = .fixed,  .len = 6   }; // Processing Code
    s[4]  = .{ .kind = .fixed,  .len = 12  }; // Amount, Transaction
    s[5]  = .{ .kind = .fixed,  .len = 12  }; // Settlement Amount
    s[7]  = .{ .kind = .fixed,  .len = 10  }; // Transmission Date & Time
    s[11] = .{ .kind = .fixed,  .len = 6   }; // STAN
    s[12] = .{ .kind = .fixed,  .len = 6   }; // Local Transaction Time
    s[13] = .{ .kind = .fixed,  .len = 4   }; // Local Transaction Date
    s[14] = .{ .kind = .fixed,  .len = 4   }; // Expiry Date
    s[22] = .{ .kind = .fixed,  .len = 3   }; // POS Entry Mode
    s[32] = .{ .kind = .llvar,  .len = 11  }; // Acquiring Institution ID
    s[37] = .{ .kind = .fixed,  .len = 12  }; // Retrieval Reference Number
    s[38] = .{ .kind = .fixed,  .len = 6   }; // Authorization ID Response
    s[39] = .{ .kind = .fixed,  .len = 2   }; // Response Code
    s[41] = .{ .kind = .fixed,  .len = 8   }; // Card Acceptor Terminal ID
    s[42] = .{ .kind = .fixed,  .len = 15  }; // Card Acceptor ID Code
    s[43] = .{ .kind = .fixed,  .len = 40  }; // Card Acceptor Name/Location
    s[49] = .{ .kind = .fixed,  .len = 3   }; // Currency Code
    s[102] = .{ .kind = .llvar, .len = 28  }; // Account Identification 1
    s[103] = .{ .kind = .llvar, .len = 28  }; // Account Identification 2

    return s;
}

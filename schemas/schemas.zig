// MoMo adapter schemas
pub const mtn_momo    = @embedFile("momo/mtn_momo.toml");
pub const airtel_money = @embedFile("momo/airtel_money.toml");

// ISO 8583 dialect schemas
pub const iso8583 = struct {
    pub const gimac      = @embedFile("iso8583/gimac.toml");
    pub const visa       = @embedFile("iso8583/visa.toml");
    pub const mastercard = @embedFile("iso8583/mastercard.toml");
};

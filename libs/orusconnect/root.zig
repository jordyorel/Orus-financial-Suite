pub const http_server = @import("http_server.zig");
pub const router = @import("router.zig");
pub const toml = @import("toml.zig");
pub const broker_client = @import("broker_client.zig");

pub const auth = struct {
    pub const hmac = @import("auth/hmac.zig");
    pub const oauth2 = @import("auth/oauth2.zig");
    pub const api_key = @import("auth/api_key.zig");
};

pub const adapters = struct {
    pub const mtn_momo    = @import("adapters/mtn_momo.zig");
    pub const airtel_money = @import("adapters/airtel_money.zig");
    pub const generic_momo = @import("adapters/generic_momo.zig");
};

pub const state = struct {
    pub const pending_tx = @import("state/pending_tx.zig");
};

pub const outbound = struct {
    pub const api_client = @import("outbound/api_client.zig");
};

// Top-level re-exports
pub const Server        = http_server.Server;
pub const Config        = http_server.Config;
pub const Router        = router.Router;
pub const BrokerClient  = broker_client.BrokerClient;
pub const Publisher     = broker_client.Publisher;
pub const PendingTxStore = state.pending_tx.PendingTxStore;
pub const ApiClient     = outbound.api_client.ApiClient;

test {
    _ = @import("auth/hmac.zig");
    _ = @import("auth/oauth2.zig");
    _ = @import("auth/api_key.zig");
    _ = @import("state/pending_tx.zig");
    _ = @import("toml.zig");
    _ = @import("router.zig");
    _ = @import("adapters/mtn_momo.zig");
    _ = @import("adapters/airtel_money.zig");
    _ = @import("adapters/generic_momo.zig");
}

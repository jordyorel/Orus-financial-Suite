// OrusBroker wire protocol constants.
//
// Every frame (both directions) starts with a 4-byte body length header:
//   [u32 BE: body_len]  -- byte count of the body, NOT including this header
//   [u8:  frame_type]
//   ... frame-specific payload ...
//
// Exception: PUBLISH_ACK is a single bare byte (no header) — legacy wire compat.
//
// Frame layouts (body = everything after the u32 header):
//   PUBLISH:     [u8 0x01][u16 topic_len][topic][IM bytes]
//   SUBSCRIBE:   [u8 0x02][u16 topic_len][topic][u16 sub_id_len][sub_id]
//   DELIVER:     [u8 0x03][u16 topic_len][topic][u64 wal_offset BE][IM bytes]
//   ACK:         [u8 0x04][u64 wal_offset BE]
//   PUBLISH_ACK: [u8 0x06]  (bare byte, no body_len header)

const std = @import("std");

pub const FRAME_PUBLISH:     u8 = 0x01;
pub const FRAME_SUBSCRIBE:   u8 = 0x02;
pub const FRAME_DELIVER:     u8 = 0x03;
pub const FRAME_ACK:         u8 = 0x04;
pub const FRAME_PUBLISH_ACK: u8 = 0x06;

pub const MAX_TOPIC_LEN:  usize = 256;
pub const MAX_PAYLOAD:    usize = 1024 * 1024; // 1 MB
pub const MAX_SUB_ID_LEN: usize = 64;

// sub_id must be non-empty alphanumeric + '-' '_' to prevent path traversal.
pub fn isValidSubId(sub_id: []const u8) bool {
    if (sub_id.len == 0 or sub_id.len > MAX_SUB_ID_LEN) return false;
    for (sub_id) |c| {
        if (!std.ascii.isAlphanumeric(c) and c != '-' and c != '_') return false;
    }
    return true;
}

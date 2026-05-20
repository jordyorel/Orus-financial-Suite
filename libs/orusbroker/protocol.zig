// OrusBroker wire protocol constants.
//
// Every frame (both directions) starts with a 4-byte body length header:
//   [u32 BE: body_len]  -- byte count of the body, NOT including this header
//   [u8:  frame_type]
//   ... frame-specific payload ...
//
// Exception: PUBLISH_ACK is a single bare byte (no header) — legacy wire compat.

pub const FRAME_PUBLISH:     u8 = 0x01; // Client→Broker: [u16 topic_len][topic][IM bytes]
pub const FRAME_SUBSCRIBE:   u8 = 0x02; // Client→Broker: [u16 topic_len][topic]
pub const FRAME_DELIVER:     u8 = 0x03; // Broker→Client: [u16 topic_len][topic][IM bytes]
pub const FRAME_ACK:         u8 = 0x04; // Client→Broker: [16 bytes msg_id]
pub const FRAME_PUBLISH_ACK: u8 = 0x06; // Broker→Client: single byte, no body_len header

pub const MAX_TOPIC_LEN: usize = 256;
pub const MAX_PAYLOAD:   usize = 1024 * 1024; // 1 MB

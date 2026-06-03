// root.zig est le fichier d'entrée d'un module.
// Il "exporte" tout ce que les autres modules peuvent utiliser.
//
// `pub` = public, visible depuis l'extérieur du module
// `usingnamespace` = "prends tout ce qui est dans ce fichier et expose-le"

pub const message = @import("message.zig");

// Raccourci pratique : au lieu d'écrire orusshare.message.InternalMessage
// on pourra écrire orusshare.InternalMessage directement
pub const InternalMessage = message.InternalMessage;
pub const MessageOrigin   = message.MessageOrigin;
pub const ServiceProvider = message.ServiceProvider;

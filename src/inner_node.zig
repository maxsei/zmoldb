const std = @import("std");
const NodeSize = @import("./btree.zig").NodeSize;

pub const InnerNode = extern struct {
    h: Header,
    __slots_alignment: [__n_slots_alignment]u8 = undefined,
    _slots: [(NodeSize - __n_slots_alignment - @sizeOf(Header))]u8 = undefined,

    const Self = @This();

    const Header = extern struct {
        right_page: i32 = -1,
        lower_page: u32,
        len: u16 = 0,
    };

    pub const Slot = struct {
        key: u32,
        ptr: u32,
    };

    // pub fn insert() {}
    // pub fn select() {}

    const __n_slots_alignment = 0;
    comptime {
        const _slots = "_slots";
        const misalignment = @offsetOf(Self, _slots) % @alignOf(Slot);
        if (misalignment != 0)
            @compileError(std.fmt.comptimePrint(
                "for " ++ _slots ++ " to be cast as []InnerSlot, " ++ _slots ++ " is misalignment by {d} bytes",
                .{misalignment},
            ));
    }
    pub fn slots(self: *Self) []Slot {
        return @alignCast(std.mem.bytesAsSlice(Slot, self._slots[0 .. self.h.len * @sizeOf(Slot)]));
    }

    pub fn slotArr(self: *Self) std.ArrayListUnmanaged(Slot) {
        var arr = std.ArrayListUnmanaged(Slot).fromOwnedSlice(self.slots());
        arr.capacity = self.cap();
        return arr;
    }

    pub fn cap(self: Self) u16 {
        return self._slots.len / @sizeOf(Slot);
    }

    pub fn findKey(self: *Self, key: u32) u32 {
        const maybe_i = self.findKeyLoc(key);
        return if (maybe_i) |i| self.slots()[i].ptr else self.h.lower_page;
    }
    pub fn findKeyLoc(self: *Self, key: u32) ?usize {
        const ss = self.slots();
        const i = std.sort.upperBound(Slot, key, ss, {}, struct {
            fn lt(_: void, lhs: u32, rhs: Slot) bool {
                return lhs < rhs.key;
            }
        }.lt);
        return if (i == 0) null else i - 1;
    }

    pub fn print(self: *Self) void {
        // const digits = std.fmt.digits2(@intCast(self.slots()[self.h.len - 1].key));
        const digits = "3";
        for (self.slots()) |s| {
            std.debug.print("  {d:0>0" ++ digits ++ "}", .{s.key});
        }
        std.debug.print("\n  /", .{});
        for (self.slots()) |_| {
            std.debug.print(" \\{s:0" ++ digits ++ "}", .{""});
        }
        std.debug.print("\n", .{});
        std.debug.print("{d:0>" ++ digits ++ "}", .{self.h.lower_page});
        for (self.slots()) |s| {
            std.debug.print("  {d:0>" ++ digits ++ "}", .{s.ptr});
        }
        std.debug.print("\n", .{});
    }
};

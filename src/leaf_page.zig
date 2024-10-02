const std = @import("std");
const NodeSize = @import("./root.zig").NodeSize;
const math = std.math;

pub const LeafPage = extern struct {
    h: Header = .{},
    buf: [NodeSize - @sizeOf(Header)]u8 = undefined,

    const Self = @This();

    pub fn insert(self: *Self, key: u32, val: []const u8) !void {
        // Get insertion size (fails if val.len > sizeof Slot.len).
        const insertion_size = try Slot.insertionSize(val);

        // Find key.
        const slot_idx = self.getSlotIndex(key);
        if (slot_idx < self.h.slot_len) {
            var slot = &self.getSlots()[slot_idx];
            // Update value in place and update free space counter.
            const new_slot_len: u16 = @intCast(val.len);
            if (new_slot_len <= slot.len) {
                self.h.num_fragmented_bytes += slot.len - new_slot_len;
                slot.len = new_slot_len;
                @memcpy(self.getSlotValue(slot.*), val);
                return;
            }
            // Try to make space for new value.
            else {
                if (!(self.remaining_space() > insertion_size - slot.len)) {
                    return error.OutOfSpace;
                }
                var arr = self.slotArr();
                _ = arr.orderedRemove(slot_idx);
                self.compact();
            }
        }
        // New key, do we have enought space?
        else {
            if (self.remaining_buf() > insertion_size) {
                // Have enought space to append, do nothing.
            } else if (self.remaining_space() > insertion_size) {
                self.compact();
            } else {
                return error.OutOfSpace;
            }
        }
        // Prepend new value by growing the cell buffer from the left.
        const new_slot = Slot{
            .key = key,
            .pos = @intCast(self.buf.len - val.len),
            .len = @intCast(val.len),
        };
        self.h.cell_buf_len += new_slot.len;
        const cell_buf = self.buf[new_slot.pos..];
        @memcpy(cell_buf[0..val.len], val);
        // Insert new slot in sorted order into slot array.
        self.h.slot_len += 1;
        var arr = self.slotArr();
        arr.insertAssumeCapacity(slot_idx, new_slot);
    }

    pub fn select(self: *Self, key: u32) ?[]u8 {
        const maybe_slot_idx = std.sort.binarySearch(Slot, key, self.getSlots(), {}, struct {
            fn bs(_: void, lhs: u32, rhs: Slot) math.Order {
                return math.order(lhs, rhs.key);
            }
        }.bs);
        const slot_idx = maybe_slot_idx orelse return null;
        const slot = self.getSlots()[slot_idx];
        return self.getSlotValue(slot);
    }

    pub fn delete(self: *Self, key: u32) !void {
        const maybe_slot_idx = std.sort.binarySearch(Slot, key, self.getSlots(), {}, struct {
            fn bs(_: void, lhs: u32, rhs: Slot) math.Order {
                return math.order(lhs, rhs.key);
            }
        }.bs);
        const slot_idx = maybe_slot_idx orelse {
            return error.KeyNotFound;
        };
        const slot = self.getSlots()[slot_idx];
        var arr = self.slotArr();
        _ = arr.orderedRemove(slot_idx);
        self.h.num_fragmented_bytes += slot.len;
        return;
    }

    // compact will compact all of the cell data and set the number of
    // fragmented bytes to 0.
    fn compact(self: *Self) void {
        var tmp: [@sizeOf(@TypeOf(self.buf))]u8 = undefined; // clone self.buf
        var pos: u16 = self.buf.len;
        for (self.getSlots()) |*slot| {
            const val = self.getSlotValue(slot.*);
            pos -= @as(u16, @intCast(val.len));
            @memcpy(tmp[pos..val.len], val);
            slot.pos = pos;
        }
        @memcpy(self.buf[pos..], tmp[pos..]);
        self.h.num_fragmented_bytes = 0;
    }

    // Ensure offset of buf aligned for slots.
    comptime {
        const misalignment = @offsetOf(Self, "buf") % @alignOf(Slot);
        if (misalignment != 0)
            @compileError(std.fmt.comptimePrint(
                "for buf to be cast as []Slot, buf is misalignment by {d} bytes",
                .{misalignment},
            ));
    }
    fn getSlots(self: *Self) []Slot {
        return @alignCast(std.mem.bytesAsSlice(
            Slot,
            self.buf[0 .. self.h.slot_len * @sizeOf(Slot)],
        ));
    }

    fn slotArr(self: *Self) std.ArrayListUnmanaged(Slot) {
        var arr = std.ArrayListUnmanaged(Slot)
            .fromOwnedSlice(self.getSlots());
        const max_slots = self.buf.len / @sizeOf(Slot);
        arr.capacity = max_slots;
        return arr;
    }

    fn getSlotIndex(self: *Self, key: u32) usize {
        return std.sort.upperBound(Slot, key, self.getSlots(), {}, struct {
            fn lte(_: void, lhs: u32, rhs: Slot) bool {
                return lhs <= rhs.key;
            }
        }.lte);
    }

    fn getSlotValue(self: *Self, slot: Slot) []u8 {
        return self.buf[slot.pos..][0..slot.len];
    }

    fn remaining_buf(self: Self) u16 {
        const buf_len_used = self.h.slot_len * @sizeOf(Slot) + self.h.cell_buf_len;
        return @as(u16, @intCast(self.buf.len)) - buf_len_used;
    }
    fn remaining_space(self: Self) u16 {
        return self.remaining_buf() + self.h.num_fragmented_bytes;
    }
};

const Header = extern struct {
    // TODO: next_page unused for right now <30-09-24, Max Schulte> //
    next_page: u32 = undefined, // Effectively this a pointer to the next page.
    slot_len: u16 = 0, // The number of slots being used in a the page.
    cell_buf_len: u16 = 0, // The length used for the cell buffer.
    num_fragmented_bytes: u16 = 0, // Number of fragmented bytes in cell buffer.
};

const Slot = struct {
    key: u32,
    pos: u16,
    len: u16, // Max len of tuple is 65536

    fn insertionSize(val: []const u8) !u16 {
        return if (val.len > math.maxInt(u16))
            error.ValueToLarge
        else
            @intCast(@sizeOf(@This()) + val.len);
    }
};

test "insert with existing key, value fits in place" {
    var leaf = LeafPage{};

    const key = 10;
    const val = "test data";

    // Insert first entry
    try leaf.insert(key, val);
    try std.testing.expectEqual(leaf.h.slot_len, 1);

    // Try updating the value to something smaller or equal size
    const updated_val = "small";
    try leaf.insert(key, updated_val);
    try std.testing.expectEqual(leaf.h.slot_len, 1);

    const maybe_result = leaf.select(key);
    try std.testing.expect(maybe_result != null);
    const result = maybe_result.?;
    try std.testing.expectEqualStrings(updated_val, result);
}

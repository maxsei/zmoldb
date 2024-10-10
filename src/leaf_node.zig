const std = @import("std");
const math = std.math;
const NodeSize = @import("./btree.zig").NodeSize;

pub const LeafNode = extern struct {
    h: Header = .{},
    // buf: [NodeSize - @sizeOf(LeafNodeHeader)]u8 = undefined,
    buf: [NodeSize - @sizeOf(Header)]u8 = undefined,

    const Header = extern struct {
        right_page: i32 = -1,
        slot_len: u16 = 0, // The number of slot being used in a the page.
        cell_buf_len: u16 = 0, // The length used for the cell buffer.
        num_fragmented_bytes: u16 = 0, // Number of fragmented bytes in cell buffer.
    };

    const Slot = struct {
        key: u32,
        pos: u16,
        len: u16,

        fn insertionSize(val: []const u8) !u16 {
            return if (val.len > math.maxInt(u16))
                error.ValueToLarge
            else
                @intCast(@sizeOf(@This()) + val.len);
        }
    };

    const Self = @This();

    pub fn insert(self: *Self, key: u32, val: []const u8) !void {
        // Get insertion size (fails if val.len > sizeof Slot.len).
        const insertion_size = try Slot.insertionSize(val);
        // Find key.
        const slot_idx = self.getSlotIndex(key);
        if (slot_idx < self.h.slot_len) {
            var slot = &self.slots()[slot_idx];
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
        self.h.cell_buf_len += @intCast(val.len);
        const new_slot = Slot{
            .key = key,
            .pos = @as(u16, @intCast(self.buf.len)) - self.h.cell_buf_len,
            .len = @intCast(val.len),
        };
        const cell_buf = self.buf[new_slot.pos..];
        @memcpy(cell_buf[0..val.len], val);
        // Insert new slot in sorted order into slot array.
        {
            self.h.slot_len += 1;
            var arr = self.slotArr();
            arr.insertAssumeCapacity(slot_idx, new_slot);
        }
    }

    pub fn select(self: *Self, key: u32) ?[]const u8 {
        const maybe_slot_idx = std.sort.binarySearch(Slot, key, self.slots(), {}, struct {
            fn bs(_: void, lhs: u32, rhs: Slot) math.Order {
                return math.order(lhs, rhs.key);
            }
        }.bs);
        const slot_idx = maybe_slot_idx orelse return null;
        const slot = self.slots()[slot_idx];
        return self.getSlotValue(slot);
    }

    pub fn remove(self: *Self, key: u32) !void {
        const maybe_slot_idx = std.sort.binarySearch(Slot, key, self.slots(), {}, struct {
            fn bs(_: void, lhs: u32, rhs: Slot) math.Order {
                return math.order(lhs, rhs.key);
            }
        }.bs);
        const slot_idx = maybe_slot_idx orelse {
            return error.KeyNotFound;
        };
        const slot = self.slots()[slot_idx];
        {
            var arr = self.slotArr();
            _ = arr.orderedRemove(slot_idx);
            self.h.slot_len -= 1;
        }
        self.h.num_fragmented_bytes += slot.len;
        return;
    }

    // compact will compact all of the cell data and set the number of
    // fragmented bytes to 0.
    // XXX: could avoid a tmp buffer if we had cells as a linked list.
    pub fn compact(self: *Self) void {
        var tmp: [@sizeOf(@TypeOf(self.buf))]u8 = undefined; // clone self.buf
        var pos: u16 = self.buf.len;
        for (self.slots()) |*slot| {
            const val = self.getSlotValue(slot.*);
            pos -= @as(u16, @intCast(val.len));
            @memcpy(tmp[pos..][0..val.len], val);
            slot.pos = pos;
        }
        @memcpy(self.buf[pos..], tmp[pos..]);
        self.h.num_fragmented_bytes = 0;
        self.h.cell_buf_len = @intCast(self.buf[pos..].len);
    }

    pub fn split(self: *Self) Self {
        var other = Self{};
        const leaf_slot_len_new = self.h.slot_len / 2;
        for (self.slots()[leaf_slot_len_new..]) |slot| {
            // Add slot and cell.
            other.h.slot_len += 1;
            const new_slot = &other.slots()[other.h.slot_len - 1];
            const v = self.getSlotValue(slot);
            other.h.cell_buf_len += @intCast(v.len);
            new_slot.* = .{
                .key = slot.key,
                .pos = @as(u16, @intCast(other.buf.len)) - other.h.cell_buf_len,
                .len = @intCast(v.len),
            };
            @memcpy(other.buf[new_slot.pos..][0..v.len], v);
        }
        // Remove rest of slots, compact, and retry insert.
        self.h.slot_len = leaf_slot_len_new;
        self.compact();
        return other;
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
    pub fn slots(self: *Self) []Slot {
        return @alignCast(std.mem.bytesAsSlice(Slot, self.buf[0 .. self.h.slot_len * @sizeOf(Slot)]));
    }

    pub fn slotArr(self: *Self) std.ArrayListUnmanaged(Slot) {
        var arr = std.ArrayListUnmanaged(Slot)
            .fromOwnedSlice(self.slots());
        const max_slots = self.buf.len / @sizeOf(Slot);
        arr.capacity = max_slots;
        return arr;
    }

    pub fn getSlotIndex(self: *Self, key: u32) usize {
        return std.sort.upperBound(Slot, key, self.slots(), {}, struct {
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

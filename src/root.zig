const std = @import("std");
const testing = std.testing;
const math = std.math;
const Allocator = std.mem.Allocator;

pub const BTree = struct {
    page_id: u32,
    pager: *Pager,

    const Self = @This();

    pub fn insert(self: *Self, key: u32, val: []const u8) !void {
        const maybe_mid = try self._insert(self.page_id, key, val);
        const mid = maybe_mid orelse return;
        var new_root = InnerNode{};
        new_root.h.len = 2;
        {
            const new_root_slots = new_root.slots();
            var _root = try self.pager.read_page(self.page_id);
            new_root_slots[0] = .{
                .ptr = self.page_id,
                .key = switch (_root) {
                    .leaf => |*root| root.slots()[0].key,
                    .inner => |*root| root.slots()[0].key,
                },
            };
            new_root_slots[1] = mid;
            if (mid.key < new_root_slots[0].key) {
                new_root_slots[1] = new_root_slots[0];
                new_root_slots[0] = mid;
            }
        }
        self.page_id = try self.pager.append_page(.{ .inner = new_root });
    }
    fn _insert(self: *Self, page_id: u32, key: u32, val: []const u8) !?InnerSlot {
        var page = try self.pager.read_page(page_id);
        switch (page) {
            .inner => |*inner| {
                const slots = inner.slots();
                const i = inner.findKeyLoc(key);

                // Insert to lower layer, maybe new midpoint.
                const maybe_mid = try self._insert(slots[i].ptr, key, val);
                const mid = maybe_mid orelse return null;

                // Insert slot.
                var arr = std.ArrayListUnmanaged(InnerSlot).fromOwnedSlice(inner.slots());
                arr.capacity = inner.cap();
                arr.insertAssumeCapacity(i + 1, mid);
                inner.h.len += 1;

                // If page is full then split node and return middle slot.
                var res: ?InnerSlot = null;
                if (!(inner.h.len < inner.cap())) {
                    const inner_len_new = inner.cap() / 2;
                    var new_inner = InnerNode{ .h = .{ .len = inner.h.len - inner_len_new } };
                    @memcpy(new_inner.slots(), inner.slots()[inner_len_new..]);
                    inner.h.len = inner_len_new;
                    const new_page_ptr = try self.pager.append_page(.{ .inner = new_inner });
                    inner.h.right_page = @intCast(new_page_ptr);
                    res = InnerSlot{ .key = new_inner.slots()[0].key, .ptr = new_page_ptr };
                }
                try self.pager.write_page(page_id, .{ .inner = inner.* });
                return res;
            },
            .leaf => |*leaf| {
                var res: ?InnerSlot = null;
                // Split leaf if not enough space.
                leaf.insert(key, val) catch {
                    // Copy upper half of slots into new page.
                    var new_leaf = LeafNode{};
                    const leaf_slot_len_new = leaf.h.slot_len / 2;
                    for (leaf.slots()[leaf_slot_len_new..]) |slot| {
                        // Add slot and cell.
                        new_leaf.h.slot_len += 1;
                        const new_slot = &new_leaf.slots()[new_leaf.h.slot_len - 1];
                        const v = leaf.getSlotValue(slot);
                        new_leaf.h.cell_buf_len += @intCast(v.len);
                        new_slot.* = .{
                            .key = slot.key,
                            .pos = @as(u16, @intCast(new_leaf.buf.len)) - new_leaf.h.cell_buf_len,
                            .len = @intCast(v.len),
                        };
                        @memcpy(new_leaf.buf[new_slot.pos..][0..v.len], v);
                    }

                    // Remove rest of slots, compact, and retry insert.
                    leaf.h.slot_len = leaf_slot_len_new;
                    leaf.compact();
                    // try (if (key > new_leaf.getSlots()[0].key) new_leaf else leaf).insert(key, val);
                    if (key > new_leaf.slots()[0].key) {
                        try new_leaf.insert(key, val);
                    } else {
                        try leaf.insert(key, val);
                    }

                    // Allocate a new leaf page and fix leafs' right pointers.
                    const new_page_ptr = try self.pager.append_page(.{ .leaf = new_leaf });
                    if (leaf.h.right_page > 0) {
                        new_leaf.h.right_page = leaf.h.right_page;
                    }
                    leaf.h.right_page = @intCast(new_page_ptr);
                    res = InnerSlot{ .key = new_leaf.slots()[0].key, .ptr = new_page_ptr };
                };
                // Write leaf page out.
                try self.pager.write_page(page_id, .{ .leaf = leaf.* });
                return res;
            },
        }
        // }
    }

    pub fn select(self: Self, key: u32) ?[]const u8 {
        return self._select(self.page_id, key);
    }
    fn _select(self: Self, page_id: u32, key: u32) ?[]const u8 {
        // TODO: make this const and readonly <06-10-24, Max Schulte> //
        var page = self.pager.read_page(page_id) catch {
            return null;
        };
        switch (page) {
            .inner => |*inner| {
                const slot = inner.findKey(key);
                return self._select(slot.ptr, key);
            },
            .leaf => |*leaf| {
                return leaf.select(key);
            },
        }
    }

    const RemoveOp = union(enum) {
        remove: void,
        borrow: struct { new_key: u32 },
        merge: void,
    };

    pub fn remove(self: Self, key: u32) !void {
        switch (try self._remove(self.page_id, key)) {
            .remove, .borrow => {},
            .merge => {},
        }
    }
    fn _remove(self: Self, page_id: u32, key: u32) !RemoveOp {
        var page = try self.pager.read_page(page_id);
        const res: RemoveOp = switch (page) {
            .inner => |*inner| blk: {
                // Remove key lower in tree/
                const slot_i = inner.findKeyLoc(key);
                switch (try self._remove(inner.slots()[slot_i].ptr, key)) {
                    .remove => {},
                    .borrow => |result| {
                        // Set next slot to the new key.
                        if (slot_i + 1 < inner.slots().len) {
                            inner.slots()[slot_i + 1].key = result.new_key;
                        }
                        break :blk .{ .remove = {} };
                    },
                    .merge => {
                        // Remove next slot.
                        if (slot_i + 1 < inner.slots().len) {
                            var arr = inner.slotArr();
                            _ = arr.orderedRemove(slot_i + 1);
                            inner.h.len -= 1;
                        }
                        // Done?
                        const cutoff = inner.cap() / 2 - 1;
                        if (inner.h.len > cutoff)
                            break :blk .{ .remove = {} };

                        // Merge or borrow from neighboring inner node.
                        const right_page_id: u32 = if (inner.h.right_page > 0) @intCast(inner.h.right_page) else break :blk .{ .remove = {} };
                        const right_page = try self.pager.read_page(right_page_id);
                        var right_inner = switch (right_page) {
                            .inner => |v| v,
                            else => return error.BadPage,
                        };
                        // Borrow.
                        if (right_inner.h.len > cutoff) {
                            var arr = right_inner.slotArr();
                            const removed = arr.orderedRemove(0);
                            right_inner.h.len -= 1;
                            arr.appendAssumeCapacity(removed);
                            inner.h.len += 1;
                            try self.pager.write_page(right_page_id, .{ .inner = right_inner });
                            break :blk .{ .borrow = .{ .new_key = removed.key } };
                        }
                        // Merge.
                        else {
                            for (right_inner.slots()) |slot| {
                                var arr = inner.slotArr();
                                arr.appendAssumeCapacity(slot);
                                inner.h.len += 1;
                            }
                            try self.pager.remove_page(right_page_id);
                            right_inner.h.right_page = right_inner.h.right_page;
                            break :blk .{ .merge = {} };
                        }
                    },
                }
            },
            .leaf => |*leaf| blk: {
                // Remove key.
                try leaf.remove(key);
                // Don't need to borrow or merge.
                const cutoff = leaf.buf.len / 2 - 1;
                if (leaf.remaining_space() > cutoff) {
                    break :blk .{ .remove = {} };
                }

                // Get right page as a leaf.
                const right_page_id: u32 = if (leaf.h.right_page > 0) @intCast(leaf.h.right_page) else break :blk .{ .remove = {} };
                const right_page = try self.pager.read_page(right_page_id);
                var right_leaf = switch (right_page) {
                    .leaf => |v| v,
                    else => return error.BadPage,
                };
                // Get right guide slot.
                if (right_leaf.slots().len == 0) {
                    break :blk .{ .remove = {} };
                }
                const right_guide_slot = right_leaf.slots()[0];
                // Borrow.
                if (right_leaf.remaining_space() > cutoff) {
                    try leaf.insert(right_guide_slot.key, right_leaf.getSlotValue(right_guide_slot));
                    try right_leaf.remove(right_guide_slot.key);
                    try self.pager.write_page(right_page_id, .{ .leaf = right_leaf });
                    break :blk .{ .borrow = .{ .new_key = right_leaf.slots()[0].key } };
                }
                // Merge.
                else {
                    for (right_leaf.slots()) |slot| {
                        try leaf.insert(slot.key, right_leaf.getSlotValue(slot));
                        try right_leaf.remove(slot.key);
                    }
                    try self.pager.remove_page(right_page_id);
                    leaf.h.right_page = right_leaf.h.right_page;
                    break :blk .{ .merge = {} };
                }
            },
        };
        try self.pager.write_page(page_id, page);
        return res;
    }
};
test "BTree many insert, remove, and select smoke test" {
    var pager = Pager.init(std.testing.allocator);
    defer pager.deinit();
    var btree = BTree{ .page_id = pager.pages, .pager = &pager };
    var rng = std.rand.DefaultPrng.init(0);
    var vv: [6000]?[16]u8 = undefined;
    // Set values and btree.
    // Try and retrieve values from btree.
    for (&vv, 0..) |*v, _key| {
        const key: u32 = @intCast(_key);
        {
            var buf: [16]u8 = undefined;
            rng.random().bytes(&buf);
            v.* = buf;
            try btree.insert(key, &buf);
        }
        if (rng.random().boolean()) {
            try btree.remove(key);
            v.* = null;
        }
    }
    // Try and retrieve values from btree.
    for (&vv, 0..) |maybe_expected, _key| {
        const key: u32 = @intCast(_key);
        const actual = btree.select(key);
        if (maybe_expected) |expected| {
            if (actual == null) {
                std.debug.print("could not find key: {d}\n", .{key});
                return error.TestUnexpectedResult;
            }
            try std.testing.expectEqualSlices(u8, expected[0..], actual.?);
        } else {
            if (actual != null) {
                std.debug.print("expected key not to exist: {d}\n", .{key});
                return error.TestUnexpectedResult;
            }
        }
    }
}

const BTreeNode = union(enum) {
    inner: InnerNode,
    leaf: LeafNode,
};
pub const PageSize = 4096;
pub const NodeSize = PageSize - 4;
comptime {
    std.debug.assert(@sizeOf(BTreeNode) == PageSize);
}

pub const LeafNode = extern struct {
    h: Header = .{},
    // buf: [NodeSize - @sizeOf(LeafNodeHeader)]u8 = undefined,
    buf: [NodeSize - @sizeOf(Header)]u8 = undefined,

    const Header = extern struct {
        right_page: i32 = -1,
        slot_len: u16 = 0, // The number of slots being used in a the page.
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

    // Ensure offset of buf aligned for slots.
    comptime {
        const misalignment = @offsetOf(Self, "buf") % @alignOf(Slot);
        if (misalignment != 0)
            @compileError(std.fmt.comptimePrint(
                "for buf to be cast as []Slot, buf is misalignment by {d} bytes",
                .{misalignment},
            ));
    }
    fn slots(self: *Self) []Slot {
        return @alignCast(std.mem.bytesAsSlice(Slot, self.buf[0 .. self.h.slot_len * @sizeOf(Slot)]));
    }

    fn slotArr(self: *Self) std.ArrayListUnmanaged(Slot) {
        var arr = std.ArrayListUnmanaged(Slot)
            .fromOwnedSlice(self.slots());
        const max_slots = self.buf.len / @sizeOf(Slot);
        arr.capacity = max_slots;
        return arr;
    }

    fn getSlotIndex(self: *Self, key: u32) usize {
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

const InnerNode = extern struct {
    const __n_slots_alignment = 0;
    h: Header = .{},
    __slots_alignment: [__n_slots_alignment]u8 = undefined,
    _slots: [(NodeSize - __n_slots_alignment - @sizeOf(Header))]u8 = undefined,

    const Self = @This();

    const Header = extern struct {
        right_page: i32 = -1,
        lower_page: i32 = -1,
        len: u16 = 0,
    };

    comptime {
        const _slots = "_slots";
        const misalignment = @offsetOf(Self, _slots) % @alignOf(InnerSlot);
        if (misalignment != 0)
            @compileError(std.fmt.comptimePrint(
                "for " ++ _slots ++ " to be cast as []InnerSlot, " ++ _slots ++ " is misalignment by {d} bytes",
                .{misalignment},
            ));
    }
    fn slots(self: *Self) []InnerSlot {
        return @alignCast(std.mem.bytesAsSlice(InnerSlot, self._slots[0 .. self.h.len * @sizeOf(InnerSlot)]));
    }

    fn slotArr(self: *Self) std.ArrayListUnmanaged(InnerSlot) {
        var arr = std.ArrayListUnmanaged(InnerSlot).fromOwnedSlice(self.slots());
        arr.capacity = self.cap();
        return arr;
    }

    fn cap(self: Self) u16 {
        return self._slots.len / @sizeOf(InnerSlot);
    }

    fn findKeyLoc(self: *Self, key: u32) usize {
        const ss = self.slots();
        const _i = std.sort.upperBound(InnerSlot, key, ss, {}, struct {
            fn lt(_: void, lhs: u32, rhs: InnerSlot) bool {
                return lhs < rhs.key;
            }
        }.lt);
        return if (_i == 0) _i else _i - 1;
    }

    fn findKey(self: *Self, key: u32) InnerSlot {
        const i = self.findKeyLoc(key);
        return self.slots()[i];
    }
};
const InnerSlot = struct {
    key: u32,
    ptr: u32,
};

pub const Pager = struct {
    allocator: Allocator,
    kv: std.AutoHashMap(u32, BTreeNode),
    pages: u32 = 0,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        const kv = std.AutoHashMap(u32, BTreeNode).init(allocator);
        return Self{ .allocator = allocator, .kv = kv };
    }
    pub fn deinit(self: *Self) void {
        self.kv.deinit();
    }

    pub fn read_page(self: *Self, page_id: u32) !BTreeNode {
        return self.kv.get(page_id) orelse {
            if (page_id != 0) return error.PageFault;
            // TODO: remove this once we start using the disk <06-10-24, Max Schulte> //
            const res = try self.kv.getOrPut(page_id);
            res.value_ptr.* = .{ .leaf = LeafNode{} };
            return res.value_ptr.*;
        };
    }

    pub fn write_page(self: *Self, page_id: u32, node: BTreeNode) !void {
        const ptr = self.kv.getPtr(page_id) orelse {
            return error.PageNotFound;
        };
        ptr.* = node;
    }

    pub fn append_page(self: *Self, node: BTreeNode) !u32 {
        self.pages += 1;
        try self.kv.putNoClobber(self.pages, node);
        return self.pages;
    }

    pub fn remove_page(self: *Self, page_id: u32) !void {
        if (!self.kv.remove(page_id))
            return error.PageNotFound;
    }
};

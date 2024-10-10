const std = @import("std");
const InnerNode = @import("./inner_node.zig").InnerNode;
const LeafNode = @import("./leaf_node.zig").LeafNode;
const Pager = @import("./pager.zig").Pager;
const Allocator = std.mem.Allocator;

pub const BTreeNode = union(enum) {
    inner: InnerNode,
    leaf: LeafNode,
};
pub const PageSize = 4096;
pub const NodeSize = PageSize - 4;
comptime {
    std.debug.assert(@sizeOf(BTreeNode) == PageSize);
}

pub const BTree = struct {
    page_id: u32,
    pager: *Pager,

    const Self = @This();

    pub fn insert(self: *Self, key: u32, val: []const u8) !void {
        const promotee = try self._insert(self.page_id, key, val) orelse {
            return;
        };
        var _old_root_page = try self.pager.read_page(self.page_id);
        const old_root_guide_key: u32 = switch (_old_root_page) {
            .inner => |*x| x.slots()[0].key,
            .leaf => |*x| x.slots()[0].key,
        };
        var new_root = InnerNode{ .h = .{ .len = 1, .lower_page = undefined } };
        if (old_root_guide_key < promotee.key) {
            new_root.slots()[0] = promotee;
            new_root.h.lower_page = self.page_id;
        } else {
            new_root.slots()[0] = InnerNode.Slot{ .key = old_root_guide_key, .ptr = self.page_id };
            new_root.h.lower_page = promotee.ptr;
            // unreachable;
        }
        self.page_id = try self.pager.append_page(.{ .inner = new_root });
    }
    fn _insert(self: *Self, page_id: u32, key: u32, val: []const u8) !?InnerNode.Slot {
        var page = try self.pager.read_page(page_id);
        const res: ?InnerNode.Slot = switch (page) {
            .inner => |*inner| blk: {
                const slots = inner.slots();
                const maybe_i = inner.findKeyLoc(key);
                // Insert to lower layer, maybe new midpoint.
                const maybe_mid = try self._insert(if (maybe_i) |i| slots[i].ptr else inner.h.lower_page, key, val);
                const mid = maybe_mid orelse return null;
                // Insert slot.
                var arr = std.ArrayListUnmanaged(InnerNode.Slot).fromOwnedSlice(inner.slots());
                arr.capacity = inner.cap();
                arr.insertAssumeCapacity((maybe_i orelse 0) + 1, mid);
                inner.h.len += 1;
                // If page is full split node and return median+new page ptr.
                if (!(inner.h.len < inner.cap())) {
                    const inner_len_new = inner.cap() / 2;
                    const median = inner.slots()[inner_len_new];
                    var new_inner = InnerNode{ .h = .{
                        .len = inner.h.len - inner_len_new - 1,
                        .lower_page = median.ptr,
                    } };
                    @memcpy(new_inner.slots(), inner.slots()[inner_len_new + 1 ..]);
                    inner.h.len = inner_len_new;
                    // Allocate a new inner page and fix inners' right pointer.
                    const new_page_ptr = try self.pager.append_page(.{ .inner = new_inner });
                    if (inner.h.right_page > 0) {
                        new_inner.h.right_page = inner.h.right_page;
                    }
                    inner.h.right_page = @intCast(new_page_ptr);
                    break :blk InnerNode.Slot{ .key = median.key, .ptr = new_page_ptr };
                }
                break :blk null;
            },
            .leaf => |*leaf| blk: {
                leaf.insert(key, val) catch {
                    // Split leaf if not enough space.
                    var new_leaf = leaf.split();
                    const promotee = new_leaf.slots()[0].key;
                    // Put kv into correct leaf.
                    try (if (key > promotee) &new_leaf else leaf).insert(key, val);
                    // Allocate a new leaf page and fix leafs' right pointers.
                    const new_page_ptr = try self.pager.append_page(.{ .leaf = new_leaf });
                    if (leaf.h.right_page > 0) {
                        new_leaf.h.right_page = leaf.h.right_page;
                    }
                    leaf.h.right_page = @intCast(new_page_ptr);
                    break :blk InnerNode.Slot{ .key = promotee, .ptr = new_page_ptr };
                };
                break :blk null;
            },
        };
        // Write leaf page out.
        try self.pager.write_page(page_id, page);
        return res;
    }

    pub fn select(self: Self, key: u32) ?[]const u8 {
        return self._select(self.page_id, key);
    }
    fn _select(self: Self, page_id: u32, key: u32) ?[]const u8 {
        var page = self.pager.read_page(page_id) catch return null;
        switch (page) {
            .inner => |*inner| {
                const ptr = inner.findKey(key);
                return self._select(ptr, key);
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

    // pub fn remove(self: Self, key: u32) !void {
    //     switch (try self._remove(self.page_id, key)) {
    //         .remove, .borrow => {},
    //         .merge => {},
    //     }
    // }
    // fn _remove(self: Self, page_id: u32, key: u32) !RemoveOp {
    //     var page = try self.pager.read_page(page_id);
    //     const res: RemoveOp = switch (page) {
    //         .inner => |*inner| blk: {
    //             // Remove key lower in tree/
    //             const slot_i = inner.findKeyLoc(key);
    //             switch (try self._remove(inner.slots()[slot_i].ptr, key)) {
    //                 .remove => {},
    //                 .borrow => |result| {
    //                     // Set next slot to the new key.
    //                     if (slot_i + 1 < inner.slots().len) {
    //                         inner.slots()[slot_i + 1].key = result.new_key;
    //                     }
    //                     break :blk .{ .remove = {} };
    //                 },
    //                 .merge => {
    //                     // Remove next slot.
    //                     if (slot_i + 1 < inner.slots().len) {
    //                         var arr = inner.slotArr();
    //                         _ = arr.orderedRemove(slot_i + 1);
    //                         inner.h.len -= 1;
    //                     }
    //                     // Done?
    //                     const cutoff = inner.cap() / 2 - 1;
    //                     if (inner.h.len > cutoff)
    //                         break :blk .{ .remove = {} };

    //                     // Merge or borrow from neighboring inner node.
    //                     const right_page_id: u32 = if (inner.h.right_page > 0) @intCast(inner.h.right_page) else break :blk .{ .remove = {} };
    //                     const right_page = try self.pager.read_page(right_page_id);
    //                     var right_inner = switch (right_page) {
    //                         .inner => |v| v,
    //                         else => return error.BadPage,
    //                     };
    //                     // Borrow.
    //                     if (right_inner.h.len > cutoff) {
    //                         var arr = right_inner.slotArr();
    //                         const removed = arr.orderedRemove(0);
    //                         right_inner.h.len -= 1;
    //                         arr.appendAssumeCapacity(removed);
    //                         inner.h.len += 1;
    //                         try self.pager.write_page(right_page_id, .{ .inner = right_inner });
    //                         break :blk .{ .borrow = .{ .new_key = removed.key } };
    //                     }
    //                     // Merge.
    //                     else {
    //                         for (right_inner.slots()) |slot| {
    //                             var arr = inner.slotArr();
    //                             arr.appendAssumeCapacity(slot);
    //                             inner.h.len += 1;
    //                         }
    //                         try self.pager.remove_page(right_page_id);
    //                         right_inner.h.right_page = right_inner.h.right_page;
    //                         break :blk .{ .merge = {} };
    //                     }
    //                 },
    //             }
    //         },
    //         .leaf => |*leaf| blk: {
    //             // Remove key.
    //             try leaf.remove(key);
    //             // Don't need to borrow or merge.
    //             const cutoff = leaf.buf.len / 2 - 1;
    //             if (leaf.remaining_space() > cutoff) {
    //                 break :blk .{ .remove = {} };
    //             }

    //             // Get right page as a leaf.
    //             const right_page_id: u32 = if (leaf.h.right_page > 0) @intCast(leaf.h.right_page) else break :blk .{ .remove = {} };
    //             const right_page = try self.pager.read_page(right_page_id);
    //             var right_leaf = switch (right_page) {
    //                 .leaf => |v| v,
    //                 else => return error.BadPage,
    //             };
    //             // Get right guide slot.
    //             if (right_leaf.slots().len == 0) {
    //                 break :blk .{ .remove = {} };
    //             }
    //             const right_guide_slot = right_leaf.slots()[0];
    //             // Borrow.
    //             if (right_leaf.remaining_space() > cutoff) {
    //                 try leaf.insert(right_guide_slot.key, right_leaf.getSlotValue(right_guide_slot));
    //                 try right_leaf.remove(right_guide_slot.key);
    //                 try self.pager.write_page(right_page_id, .{ .leaf = right_leaf });
    //                 break :blk .{ .borrow = .{ .new_key = right_leaf.slots()[0].key } };
    //             }
    //             // Merge.
    //             else {
    //                 for (right_leaf.slots()) |slot| {
    //                     try leaf.insert(slot.key, right_leaf.getSlotValue(slot));
    //                     try right_leaf.remove(slot.key);
    //                 }
    //                 try self.pager.remove_page(right_page_id);
    //                 leaf.h.right_page = right_leaf.h.right_page;
    //                 break :blk .{ .merge = {} };
    //             }
    //         },
    //     };
    //     try self.pager.write_page(page_id, page);
    //     return res;
    // }
};

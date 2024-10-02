const std = @import("std");
const testing = std.testing;
const math = std.math;
const LeafPage = @import("./leaf_page.zig").LeafPage;

const B = 32;
pub const PageSize = 4096;

pub const BTree = struct {
    // root: BTreeNode,
    // page_getter: fn get_page(page: u32) *BTreeNode,

    const Self = @This();

    fn get_page(page: u32) *BTreeNode {
        _ = page;
        @panic("not implemented");
    }

    fn insert(_: Self, key: u32, val: []const u8) !void {
        std.debug.assert(key > 0);
        // TODO: calculate what the minimum storage of a value.
        std.debug.assert(val > 4000);
        var node = get_page(1);
        while (true) {
            switch (node) {
                .inner => |inner| {
                    // TODO: do binary search on inner to find the index to look
                    // for <25-09-24, Max Schulte> //
                    node = get_page(inner.nodes[0].ptr);
                },
                .leaf => |leaf| {
                    try leaf.insert(key, val);
                },
            }
        }
    }

    // fn search(self: Self, key: u32) ?[]const u8 {
    //     if (key == 0) return null;
    //     var node = get_page(1);
    //     switch (node) {
    //         .inner => |inner| {

    //         },
    //         .leaf => |leaf| {
    //             var it = leaf.iterator();
    //             while (it.next()) |v| {
    //                 // TODO: probably need to distinguish between key and value
    //                 // here <25-09-24, Max Schulte> //
    //                 const candidate_key: u32 = @ptrCast(v);
    //                 if (key == candidate_key) {
    //                     return v;
    //                 }
    //             }
    //             return null;
    //         },
    //     }
    // }

    // fn remove(self: Self) void{}
};

const InnerSlot = struct {
    key: u32,
    ptr: u32,
};

const BTreeNode = extern union {
    inner: extern struct {
        // nodes: [NodeSize / @sizeOf(InnerSlot) - 3]InnerSlot,
        nodes: [NodeSize]u8 = undefined,
    },
    leaf: LeafPage,
};
pub const NodeSize = PageSize - @sizeOf(extern union {});
comptime {
    std.debug.assert(@sizeOf(BTreeNode) == PageSize);
}

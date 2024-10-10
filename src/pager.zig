const std = @import("std");
const Allocator = std.mem.Allocator;
const BTreeNode = @import("./btree.zig").BTreeNode;
const LeafNode = @import("./leaf_node.zig").LeafNode;

pub const Pager = struct {
    allocator: Allocator,
    kv: std.AutoHashMap(u32, BTreeNode),
    pages: u32 = 0,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        const kv = std.AutoHashMap(u32, BTreeNode).init(allocator);
        return .{ .allocator = allocator, .kv = kv };
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

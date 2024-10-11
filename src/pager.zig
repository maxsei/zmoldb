const std = @import("std");
const Allocator = std.mem.Allocator;
const BTreeNode = @import("./btree.zig").BTreeNode;

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
            return error.PageFault;
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

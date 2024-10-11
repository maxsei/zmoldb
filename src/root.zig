const std = @import("std");
const testing = std.testing;
const math = std.math;
const BTree = @import("./btree.zig").BTree;
const Pager = @import("./pager.zig").Pager;

test "BTree many insert, remove, and select smoke test" {
    var pager = Pager.init(std.testing.allocator);
    defer pager.deinit();
    const root_page_id = try pager.append_page(.{ .leaf = .{} });
    var btree = BTree{ .page_id = root_page_id, .pager = &pager };
    var rng = std.rand.DefaultPrng.init(0);
    var vv: [12000]?[16]u8 = undefined;
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
        // if (rng.random().boolean()) {
        //     try btree.remove(key);
        //     v.* = null;
        // }
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

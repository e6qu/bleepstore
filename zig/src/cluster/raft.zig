const std = @import("std");

/// Raft node role in the cluster.
pub const Role = enum {
    follower,
    candidate,
    leader,
};

/// A single entry in the Raft log.
pub const LogEntry = struct {
    term: u64,
    index: u64,
    data: []const u8,
};

/// Raft consensus node.
///
/// Manages leader election, log replication, and state machine application
/// for the BleepStore cluster. This is a stub; the full Raft protocol
/// implementation is pending.
pub const RaftNode = struct {
    allocator: std.mem.Allocator,
    node_id: u64,
    role: Role,
    current_term: u64,
    voted_for: ?u64,
    log: std.ArrayList(LogEntry),
    commit_index: u64,
    last_applied: u64,
    peers: []const []const u8,
    running: bool,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, node_id: u64, peers: []const []const u8) Self {
        return Self{
            .allocator = allocator,
            .node_id = node_id,
            .role = .follower,
            .current_term = 0,
            .voted_for = null,
            .log = .empty,
            .commit_index = 0,
            .last_applied = 0,
            .peers = peers,
            .running = false,
        };
    }

    pub fn deinit(self: *Self) void {
        self.log.deinit(self.allocator);
    }

    /// Start the Raft node: begin participating in leader election and
    /// log replication.
    pub fn start(self: *Self) !void {
        if (self.running) return error.AlreadyRunning;
        self.running = true;
        std.log.info("raft node {d} started as follower (term {d})", .{ self.node_id, self.current_term });
        // TODO: start election timer, heartbeat loop, RPC listener
    }

    /// Stop the Raft node gracefully.
    pub fn stop(self: *Self) void {
        self.running = false;
        std.log.info("raft node {d} stopped", .{self.node_id});
    }

    /// Propose a new entry to the Raft log.
    /// Only the leader may accept proposals; followers should redirect.
    pub fn apply(self: *Self, data: []const u8) !void {
        if (!self.running) return error.NotRunning;
        if (self.role != .leader) return error.NotLeader;

        const entry = LogEntry{
            .term = self.current_term,
            .index = self.log.items.len + 1,
            .data = data,
        };
        try self.log.append(self.allocator, entry);

        std.log.info("raft node {d}: appended log entry index={d} term={d}", .{
            self.node_id,
            entry.index,
            entry.term,
        });

        // TODO: replicate to peers, advance commit_index on majority ack
    }

    /// Transition to candidate and start an election.
    pub fn startElection(self: *Self) !void {
        if (!self.running) return error.NotRunning;

        self.current_term += 1;
        self.role = .candidate;
        self.voted_for = self.node_id;

        std.log.info("raft node {d}: starting election for term {d}", .{
            self.node_id,
            self.current_term,
        });

        // TODO: send RequestVote RPCs to peers
    }

    /// Become the leader (called after winning an election).
    pub fn becomeLeader(self: *Self) void {
        self.role = .leader;
        std.log.info("raft node {d}: became leader for term {d}", .{
            self.node_id,
            self.current_term,
        });

        // TODO: send initial empty AppendEntries (heartbeat) to all peers
    }

    /// Step down to follower, e.g. upon receiving a higher term.
    pub fn stepDown(self: *Self, new_term: u64) void {
        self.current_term = new_term;
        self.role = .follower;
        self.voted_for = null;

        std.log.info("raft node {d}: stepped down to follower at term {d}", .{
            self.node_id,
            self.current_term,
        });
    }
};

test "RaftNode init and start" {
    const allocator = std.testing.allocator;
    var node = RaftNode.init(allocator, 1, &.{});
    defer node.deinit();

    try std.testing.expectEqual(Role.follower, node.role);
    try std.testing.expectEqual(@as(u64, 0), node.current_term);
    try std.testing.expect(!node.running);

    try node.start();
    try std.testing.expect(node.running);

    node.stop();
    try std.testing.expect(!node.running);
}

test "RaftNode apply requires leader" {
    const allocator = std.testing.allocator;
    var node = RaftNode.init(allocator, 1, &.{});
    defer node.deinit();

    try node.start();
    defer node.stop();

    // Follower cannot apply.
    try std.testing.expectError(error.NotLeader, node.apply("test"));
}

test "RaftNode election flow" {
    const allocator = std.testing.allocator;
    var node = RaftNode.init(allocator, 1, &.{});
    defer node.deinit();

    try node.start();
    defer node.stop();

    try node.startElection();
    try std.testing.expectEqual(Role.candidate, node.role);
    try std.testing.expectEqual(@as(u64, 1), node.current_term);

    node.becomeLeader();
    try std.testing.expectEqual(Role.leader, node.role);

    try node.apply("hello");
    try std.testing.expectEqual(@as(usize, 1), node.log.items.len);
}

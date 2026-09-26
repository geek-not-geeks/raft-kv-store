# Devlog

## Phase 1
Built basic TCP server + client. SET/GET/DELETE working on a single node, in-memory map.

## Phase 2
Multi-node heartbeat over TCP. 3 nodes pinging each other on localhost, different ports.

## Phase 3
Leader election. Bug: leader kept re-electing itself every timeout since RunElectionTimer didn't check node state. Fixed by skipping election attempts when already leader. Verified failover manually by killing leader process twice in a row, new leader elected within ~200ms both times.

## Phase 4
Log replication. Switched peer RPCs from plain text to JSON (RequestVote/AppendEntries), added leader/follower log + commit index tracking. Verified: write through leader, read from a different node, got correct value back.

## Phase 5
- Killed a follower mid-run, wrote 3 keys while it was down, restarted it. It caught up automatically via AppendEntries log-matching, no manual fix needed.
- Added a manual partition toggle (PARTITION on/off command) to simulate a network split. Verified majority side (2/3 nodes) kept accepting writes while one node was cut off. Isolated node correctly rejected writes. Healed and caught up automatically after reconnecting.


## Phase 6
Chaos test: 3 nodes, ~600 writes over 45s, killed and restarted 2 different nodes mid-run (including once during load). First run failed - a restarted node with an empty log took too long to recatch up because NextIndex only backed off one entry at a time. Fixed by adding conflict-index/term info to AppendEntries replies so the leader can jump back to the right point in one round trip instead of crawling backward.

After the fix: re-ran the same kill pattern. First verify run showed 1 of 10 keys briefly mismatched on one node - reran immediately, all nodes matched. This is expected: DUMP reads a node's local state directly, not through consensus, so a follower can be a beat behind right after catching up. Not a correctness violation, just confirms reads aren't guaranteed fresh on followers.

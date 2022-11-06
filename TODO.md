# TODO

- problem: delete and after that add the same block with no GC on the middle. Tombstone will have the hash, so when executing GC we will consider it as deleted.
- implement hardcoded Queries
- use the prefix on the key to create packfiles on different namespaces. Example: instead of hashing the entire key `/my/key/VALUE` split the key in two: `/my/key` and `VALUE`. Doing that the lookout for the key will be much faster.
- DiskUsage: get stats from packfiles, indexes and the thombstone. Get also stats from pending files.
- GC: check TODO list
- Index:
    - Add block size into the index to speed up size lookups
    - Packfile checksum
    - Index checksum
- Packfiles:
    - Add a performant join
- Tombstone:
    - Avoid to have everything on memory

NEXT TASK: Implement GC to move values from objectStorage to packfiles
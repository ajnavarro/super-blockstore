# TODO

- problem: delete and after that add the same block with no GC on the middle. Tombstone will have the hash, so when executing GC we will consider it as deleted.
- implement hardcoded Queries.
- use the prefix on the key to create packfiles on different namespaces. Example: instead of hashing the entire key `/my/key/VALUE` split the key in two: `/my/key` and `VALUE`. Doing that the lookout for the key will be much faster. PROBLEM: batch must support several packfiles at the same time.

- GC: check TODO list
- Index:
    - Packfile checksum
    - Index checksum
- Packfiles:
    - Add a performant join
- Tombstone:
    - Avoid to have everything on memory

TODO: 
- do a lookup on all the indexes at the same time
- research about compressing the entire packfile using s3 instead of each element
- add MIDX: https://git-scm.com/docs/pack-format#_multi_pack_index_midx_files_have_the_following_format
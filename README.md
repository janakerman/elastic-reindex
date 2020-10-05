# Elastic Reindex

A demonstration of a method to reindex an index receiving live updates using write duplication.

To ensure a no downtime modifications of a primary index receive under live search load (index A), we can duplicate writes to
a secondary index (index B), keeping index as the target of search queries. In the background, we can perform some
use the Reindex API to bring B in sync with A. When index B is in sync, we can switch search traffic to B and delete index A.

* Reading from an index alias would allow use of the Alias API to easily switch reads from A to B.
* A heavily throttled reindex operation can continue in the background for as long as is necessary (he Reindex API provides
settings for throttling). 
* We can completely turn off index refreshing from B during the reindex operations and enabled when reindex is complete.
* This method is useful for modifying settings on indexes that cannot be modified after creation (i.e shard count). 

## Example

```
docker-compose up -d
go test
```

Output:
```
=== RUN   TestReindex
Set the read index and primary index to index 'a'
Start indexing documents into index 'a'
A[num: 0 - ID: ] B[num: 0 - ID: ]
A[num: 273 - ID: 272] B[num: 0 - ID: ]
A[num: 821 - ID: 820] B[num: 0 - ID: ]
Add secondary index to start duplicating writes to index 'b'
A[num: 1319 - ID: 1318] B[num: 0 - ID: ]
A[num: 1622 - ID: 1621] B[num: 98 - ID: 1614]
A[num: 1834 - ID: 1833] B[num: 310 - ID: 1826]
Reindex 'a' into 'b'
A[num: 2044 - ID: 2043] B[num: 519 - ID: 2035]
A[num: 2230 - ID: 2229] B[num: 2224 - ID: 2223]
A[num: 2409 - ID: 2408] B[num: 2404 - ID: 2403]
A[num: 2579 - ID: 2578] B[num: 2572 - ID: 2571]
Stop indexing documents so we can see indexes settle
Index 'a' and 'b' should be in sync
A[num: 2664 - ID: 2663] B[num: 2664 - ID: 2663]
A[num: 2664 - ID: 2663] B[num: 2664 - ID: 2663]
Start indexing documents again
A[num: 2664 - ID: 2663] B[num: 2664 - ID: 2663]
A[num: 2776 - ID: 2775] B[num: 2772 - ID: 2771]
A[num: 2965 - ID: 2964] B[num: 2961 - ID: 2960]
Switch read index to index 'b' (could be an alias)
Reads are now made to index 'b'
A[num: 3136 - ID: 3135] B[num: 3132 - ID: 3131]
A[num: 3302 - ID: 3301] B[num: 3298 - ID: 3297]
A[num: 3464 - ID: 3463] B[num: 3460 - ID: 3459]
Switch the primary index to index 'b'
Primary index set to 'b'
A[num: 3625 - ID: 3624] B[num: 3622 - ID: 3621]
A[num: 3682 - ID: 3681] B[num: 3874 - ID: 3874]
A[num: 3682 - ID: 3681] B[num: 4188 - ID: 4188]
Delete index 'a'
A[num: 0 - ID: ] B[num: 4492 - ID: 4492]
A[num: 0 - ID: ] B[num: 4687 - ID: 4687]
A[num: 0 - ID: ] B[num: 4979 - ID: 4979]
Stop indexing documents
A[num: 0 - ID: ] B[num: 5251 - ID: 5251]
A[num: 0 - ID: ] B[num: 5334 - ID: 5334]
A[num: 0 - ID: ] B[num: 5334 - ID: 5334]
--- PASS: TestReindex (54.23s)
PASS
```
# Fast tenant transfers for high availability

- Author: john@neon.tech
- Created on 2023-08-11
- Implemented on ..

## Summary

The preceding generation numbers RFC may be thought of as "making tenant
transfers safe". Following that,
this RFC is about how those transfers are to be done _quickly_ and _efficiently_.

In this context, doing a migration "quickly" means doing it with least possible
window of disruption to workloads in the event that we are migrating because
of a failure.

This is accomplished by introducing two high level changes:

- A dual-attached state for tenants, used in a control-plane-orchestrated
  migration procedure that preserves availability during a migration.
- Warm secondary locations for tenants, where on-disk content is primed
  for a fast migration of the tenant from its current attachment to this
  secondary location.

## Motivation

Migrating tenants between pageservers is essential to operating a service
at scale, in several contexts:

1. Responding to a pageserver node failure by migrating tenants to other pageservers
2. Balancing load and capacity across pageservers, for example when a user expands their
   database and they need to migrate to a pageserver with more capacity.
3. Restarting pageservers for upgrades and maintenance

In all of these cases, users expect the service to minimize any loss of availability. However,
in the case of proactive movement initiated intentionally, users reasonably expect absolutely
no noticeable loss of availability. If transfers have a user impact (as they currently do), we are de-motivated
from otherwise useful proactive tenant movement, e.g. for balancing load. Therefore making
tenant movement low-impact for users doesn't just make failovers fasterr: it unlocks
the ability to do more continuous management of workload on pageservers using
proactive migrations from one healthy node to another.

Currently, a tenant may be re-attached by detaching it from an old node and
attaching it to a new one. Once the generation numbers RFC is implemented,
it is also safe to attach a tenant to a new pageserver without detaching
it from the old pageserver (for example if the old pageserver is unresponsive,
stuck in terminating state, or network partitioned).

Let us consider the pageserver client's view (i.e. postgres's view) when we do such a detach/attach
cycle:

- Between the detach and attach, no reads may be serviced
- After attach, LSNs above the remote consistent lsn will will not
  be readable until the new pageserver has replayed the WAL up to the
  present point.
- After attach, reads will have much higher latency until the new pageserver
  has populated its local disk with any layers that the client is reading from.
  For a client doing random reads, this will require loading at least the total size
  of the database in image layers, plus whatever delta layers are required.

Those availability windows are substantial, and the purpose of this RFC is
to define how to close availability gaps in two ways:

- Remove the inter-attachment availability gap by using multi-attachment of
  tenants during a migration.
- Remove the cold-cache availability/latency gap by adding secondary
  locations for tenants that will have pre-warmed caches.

## Non Goals (if relevant)

- We do not aim to have the pageservers fail over if the
  control plane is unavailable.
- On unplanned migrations (node failures), we do not aim to prevent a small, bounded window of
  read unavailability of very recent LSNs (because postgres
  page cache usually contains such pages, we do not expect
  them to be read frequently from the pageserver).

## Impacted components (e.g. pageserver, safekeeper, console, etc)

Pageserver, control plane

## Proposed implementation

Since the Generation Numbers RFC enables safe multi-attachment of tenants, we may exploit
this for seamless migration.

The high level flow is:

- Attach the tenant to both old and new pageservers
- Wait for the new pageserver to catch up to an LSN as least as recent as the old one
- Update Endpoints to use the new pageserver
- Detach the old pageserver

In addition to that migration flow, the new pageserver will have a warm cache, by being
identified as a "secondary warm location" by the control plane, and continuously
downloading layers from S3 in the background in case it needs to receive an incoming
migration quickly.

### Cutover procedure

The exact steps to do the neatest possible cutover depend on whether we are migrating from a healthy node,
or from a degraded/unresponsive node. However, to avoid defining distinct procedures
for these cases, we will define one procedure with certain optional/advisory steps. The
correctness parts of the procedure are the same for any tenant migration.

To accomodate the need for some attachments to behave differently while multi-attached, we
will break up the current "attached" state into several states:

- **AttachedSingle**: our current definition of "attached": the attached node has exclusive
  control over the tenant.
- **AttachedMulti**: Uploads are permitted but deletions are blocked.

If a node is in AttachedSingle and witnesses a higher generation number for the tenant,
it will automatically switch into AttachedMulti: for a node with a stale generation number,
AttachedMulti is equivalent to failing the pre-deletion checks introduced in the
Generation Numbers RFC, but at a higher level where we will avoid even trying to
delete anything, or to do compaction of remote data.

A node in AttachedMulti with
a stale generation number will also avoid doing any S3 uploads, as it knows that
since its generation is stale, future generations will not see uploads in that
generation. This last part is not necessary for correctness but avoids writing
useless objects to S3.

The AttachedMulti state is needed to preserve availability of reads on node A during
the migration, otherwise node B might execute deletions that affect objects node A's generation's index still refers to. It is not necessary for data integrity: if the
control plane put two nodes concurrently into AttachSingle, the generation number safety
rules would avoid any corruption, but the older-generationed attachment could see
its remote objects getting deleted by the newer generationed attachment, and thereby
become unavailable for reads.

For a migration from old node A to new node B:

1. (Optional, may fail) RPC to node A requesting that it flush to S3.
2. Increment attachment generation number and attach node B in state AttachedMulti
3. (Optional, may fail) RPC to node A notifying it of the new generation. If this
   step fails, then node A will still eventually fail to do deletions when it fails to validate
   its attachment generation number (see Generation Numbers RFC). If this step succeeds,
   node A will stop all S3 writes since its generation is stale, but remain available for reads.
4. (Optional) If node A was available in previous step, then RPC to node B requesting that it download
   the full latest image layer, and wait for this to complete before proceeding.
5. Enter catchup polling loop, where we read the latest visible LSN from both nodes, and proceed
   when node B's LSN catches up to node A's LSN, or when node A does not respond.
6. Update Endpoints to use node B instead of node A.
7. RPC to node B to set state AttachedSingle
8. (Optional, may fail) RPC to node A to detach.

We have two different user experiences depending on whether node A was responsive:

- The guaranteed end state, if node B is behaving, is that the endpoint ends up talking to a pageserver
  that will eventually serve all its reads when it catches up with the WAL.
- The happy path end state is that we waited until node B caught up with node A before cutting over, so
  there was no degredation to the endpoint's experience associated with the cutover: any LSN gap it sees
  is no worse on node B than it was on node A.

The "optional" steps can be proactively skipped if the control plane believes node A is offline, so
that we avoid N different tenant migrations all trying+failing to execute the optional RPCs.

#### Recovering from failures during cutover

We may be unlucky, and node B fails while we are cutting over. The mostly likely reason for this would
be a cascading failure, if we have overloaded a node that we are trying to migrate work to: we should endeavor
to avoid this, but also design to handle it safely.

If we save the generation numbers of the nodes when we started a migration in the control plane
database, then the control plane's reconcilation loop may reliably "notice" if a node that was
participating in a migration is no longer alive.

We may _not_ "fail back" to make node A an AttachedSingle under its original generation number,
because node B may have already written their own metadata to remote storage, and any subsequent
generation would end up reading node B's metadata instead of node A's.

We may ask node A to do a quick "detach/attach" cycle to transition from its old generation to a
new generation, but that is only useful if node A is available and healthy (there was probably some
reason we were migrating _away_ from it to begin with). If Nodes A and B are both fenced, then there
is nothing to recover from, and we can simply put a new node directly into AttachedSingleWriter
with a new generation number. In this context "fenced" means that their node generation number is higher
than the generation number we recorded at the start of the migration.

Given that destination node failures during a migration will be rare events, rather than providing
a fine-grained set of recovery plans for all possible failure states, we may adopt a simple model
with three procedures depending on the state of node A and the purpose of the migration:

- If the node A was offline (failover case), then immediately select a new pageserver for the tenant, and
  attach it in AttachedSingle. It will download from S3 and try to catch up with the WAL: there will
  be some unavoidable availability gap to clients, since we suffered a double node failure (node A failed,
  then node B failed while we were migrating node A's work)
- If we were migrating for a node evacuation, and node A is still online, then ensure endpoints are still directed at
  node A, and then identify a new migration destination based on the same pageserver selection logic
  used for new tenants, attach to that new destination in AttachedMulti, and resume the migration
  procedure.
- If we were migrating for a specific API request asking to move a tenant to a particular node, then
  detach from node A and re-attach in state AttachedSingle. This will have a brief availability gap,
  but that's tolerable because this scenario is very rare (a node failure while we were manually
  migrating a tenant). We could in principle avoid this gap with a special case in the pageserver code
  to fast-forward an attachment's generation without detaching, but it is not good value-for-money in complexity.

At the end of the migration operation:

- the tenant will be available, assuming there was at least one healthy pageserver with enough capacity available.
- the actual location where it ends up depends on whether there was a failure during migration, and whether the operation was a node evacuation or an explicit point-to-point movement of a tenant.
- the attached pageserver with the highest generation number will be in state AttachedSingle.

## Warm secondary locations

**Terminology note**: the term "warm standby" is overloaded (also has a meaning in postgres), so
we avoid it here and refer to "warm secondary locations".

We introduce the concept of a warm secondary location, meaning pageservers to which
a tenant may be attached very quickly due to having pre-warmed content on local disk.

A tenant may have multiple warm secondary locations. If this set includes the currently
attached pageserver, then the attachment "wins" and the node will only act as a warm secondary
location when no longer attached.

The secondary location's job is to serve reads **with the same quality of service as the original location
was serving them around the time of a migration**. This does not mean the secondary
location needs the whole database downloaded:

- If the old location has not served any read requests recently, then the warm secondary
  may not need any local content at all: it can "be idle" just as well as the primary was
  being idle, without any local data.
- If the old location's pattern of client reads was concentrated on certain layers, then
  the secondary only needs to preload those layers, not all content for the tenant.

**Terminology note**: an "attachment" still means a pageserver that is attached for reads
and writes: a warm secondary location is not considered attached, and does not have
an attachment generation number. The tenant is only attached to one pageserver (unless
it is double-attached during a migration),

## Layer heat map

The attached page server is responsible for notifying all secondary locations of
enough information to decide which layers to download to their local storage.

This could be done by simply sending a list of layers that the attached node deems
hot enough, but this prevents the secondary from applying its own policy about
how much to download from each tenant. For example, a secondary may have
a storage limitation that motivates only downloading the hottest layers from
all tenants.

The heat map may initially be quite simple: a collection of layer names, with
a heat for each one. The heat may be a time-decaying counter of page reads to
the layer. The heat map only includes layers which have a nonzero counter, where
some threshold is set for clamping counters to zero when they decay past it. This
threshold may be set based on some time policy, such as keeping something warm
for 10 minutes, i.e. ensuring that a layer which had 1 read 11 minutes ago would
have a zero heat.

Secondary locations may combine the heat map with knowledge of layer sizes to
determine a "cost/benefit ratio" to pre-downloading a layer, and prioritize
downloads based on that.

Over time, the heat map may be evolved to account for more complex understanding
of access patterns and costs.

## Local attachment metadata

To facilitate the finer-grained states that a tenant may have within a pageserver (beyond
just being attached or not), without requiring synchronization with control plane on
startup, a metadata file will be written to the tenant's directory in local storage,
updated when events such as attach/detach happen.

- To distinguish between AttachedSingle, and AttachedMulti
- to retain a memory of the latest generation seen as well as the attached generation,
  we should write a file to local storage within the tenant directory.
- To distinguish between warm secondary locations and attachments: these will
  be the same layer files in the same directory hierarchy, so that when we switch
  between secondary status & attached status, we don't move anything around.

This might later evolve into more of a manifest of the contents of local storage,
but for now it is just an O(1) record of the details of the attachment, now
that an attachment

## Secondary download/housekeeping logic

Secondary warm locations run a simple loop, implemented separately from
the main `Tenant` type, which represents attached tenants:

- Periodically RPC to the attached pageserver (learn this via storage broker) to retrieve layer heatmap
- Select any "hot enough" layers to download, if there is sufficient
  free disk space.
- Download layers
- Download the latest index_part.jsons
- Check if any layers currently on disk are no longer referenced by
  the tenant's metadata & delete them

## Configuring secondary locations

A new API `/v1/tenants/:tenant_id/configure` will be added, to enable configuring
a particular tenant's behavior on a node in a more general way than just attaching
or detaching it.

Sending a DELETE to this request will cause the pageserver to stop holding
a warm cache for the tenant. A DELETE will get a `400` response if the tenant
is currently attached to this node: the control plane must detach it first.

## Promotion/demotion from warm secondary to/from attachment

No special behavior is required from the control plane. The pageserver will remember
what was previously done with the `/configure` API, such that if a tenant was configured
as a secondary location, then attached, then the following detach will revert it to
a secondary location and the cache keep-warm loop will reactivate.

If a node is in use as a secondary location, then a normal `/v1/tenants/:tenant_id/attach`
API request may be made, and the pageserver will internally manage the transition
from running the keep-warm loop to running a fully attached tenant.

## Correctness

### Keep-warm downloads are advisory only

We avoid relying on secondaries for any correctness guarantees other than
that the layers it has downloaded will match the layers written to remote storage.

More specifically:

- The secondary is not required to download everything in the heat map: it may
  autonomously deprioritize this work and/or reclaim disk space
- The secondary is not required to meet any freshness requirement for data
  or metadata: when a tenant is attached, it is the attachment that is required
  to synchronize with remote metadata before proceeding.

### Preserving read availability on old pageserver, during migration.

Generation numbers provide storage safety for attaching to a new pageserver without
detaching the old one, but one extra change is needed to preserve availability on
the previous attachment while this is going on: the new pageserver must not do
any deletions until we no longer want the old pageserver to be able to serve reads.

This corresponds to the period where we have
two attachments that may both serve reads, but the old attachment has more recent
LSNs readable than the new attachment.

The old attachment is prevented from doing deletions implicitly because the new attachment
has gained a higher generation number.

## Alternatives considered

### Pageserver-granularity failover

Instead of migrating tenants individually, we could have entire spare nodes,
and on a node death, move all its work to one of these spares.

This approach is avoided for several reasons:

- we would still need fine-grained tenant migration for other
  purposes such as balancing load
- by sharing the spare capacity over many peers rather than one spare node,
  these peers may use the capacity for other purposes, until it is needed
  to handle migrated tenants. e.g. for keeping a deeper cache of their
  attached tenants.

### Cold secondary locations

We could implement all migration logic without implementing warm secondaries:
this would not hurt availability during planned migrations, but would make
unplanned migrations (i.e. failover on node death) unacceptably slow, as
peers would have to download hundreds of GiB from S3 before resuming
service.

### Hot secondary locations

Instead of our "warm" secondary locations that only download remote data, we
could instead use "hot" secondary locations that continuously replay the
WAL to local storage. This would provide even faster transfers, at the cost
of imposing double load on the safekeepers all the time (not just during
a migration), and requiring more local disk bandwidth to stream all
WAL writes into delta L0 layers and compact them.

A hot secondary location would have faster cutover, but this only matters in
the event of a total pageserver failure: for planned migrations where the original
pageserver is online, warm migrations present the same level of availability
and performance to clients, they just take slightly longer to happen.

### Readonly during migration

We could simplify migrations by making both previous and new nodes go into a
readonly state, then flush remote content from the previous node, then activate
attachment on the secondary node.

The downside to this approach is a potentially large gap in readability of
recent LSNs while loading data onto the new node. To avoid this, it is worthwhile
to incur the extra cost of double-replaying the WAL onto old and new nodes' local
storage during a transfer.

### Warm Secondary Locations

The cutover process defined above is correct, and preserves availability, but may not be fast: if
the new pageserver doesn't have anything in its local cache, then completing a migration for a large
database will take a time proportional to the database's size.

Fast cutovers are important when responding to an unexpected node failure, or when migrating load
to respond to a spike.

Our architecture relies on having a good cache hit rate on local disk when serving reads and doing compaction,
so providing a fast failover may only be accomplished by having a similarly good cache hit rate on
a new location, by pre-populating it with data.

### Control Plane Changes

This section is speculative, the exact implementation may vary.

#### Database schema

Currently the `Project` object in the control plane database schema only
carries single pageserver ID. This will be extended to represent multiple attachment,
and to represent a list of locations.

Each location should record:

- u32: Attachment generation number (may be null if never attached)
- enum(attached/attaching/detached): whether currently attached. If in attaching state,
  then we are pending sending an `/attach` call to the pageserver.
- bool: whether in use by endpoints.

#### Driving migrations

Currently, there is a `tenant_migrate` endpoint in the v2 mgmt API, which
is implemented as a series of individual `Operation` records that suspend
the endpoint, disable ("ignore") the tenant on the original server,
attach it to the new one, resume the compute, and finally detach from
the old server.

We may adapt that existing structure to the new migration procedure, with a new
migration `Operation` that encloses multiple steps -- the procedure described in
this RFC would be quite awkward to implement as separate steps, as if there are failures
then we may change our mind about the ultimate destination of the migration.

It may make sense to have a separate `Operation` type for bulk movements, so that
when draining a node prior to maintenance, we can dynamically choose where to send
tenants based on load over a series of batches, rather than having to choose all
destinations up front.

Migration operations may have two variants:

- Transient: move attachments, but leave secondary warm locations behind: we
  expect that this original server will be used again.
- Permanent: move attachments away, and also try to find new secondary warm locations
  for tenants that were using this pageserver. The end state is that this pageserver
  has no attachments and is also not a location for any tenant.

#### Handling unresponsive pageservers

The `Project` object should have a column to track whether a migration is in process:
during a migration the `Operation` for the migration is responsible for all attach/detach
calls out to pageservers. Outside of that operation, some reconcilation process should identify
pageservers that have stale attachments: e.g. if we migrated from an unavailable pageserver and it
later becomes available, we should detach from it.

Migrations treat detaching from the old pageserver as optional, so after a migration
triggered by node failure, we may leave the Project in a state where "previous pageserver
still attached" column is true.

Some background process will be needed to reconcile this when the pageserver eventually
comes back online. Also, if we add a way to administratively delete a pageserver in
the

#### Interaction with Endpoints

Currently, endpoints are suspended and then started during a migration: the endpoints
must be adapted to accept a configuration change at runtime.

This RFC just deals with pageservers: runtime modification of endpoint configuration
is out of scope, but believed to be a realistic change to make in the near future.

#### Managing warm secondary locations

Warm secondary locations operate similarly to attachments: to set one, it must be
written to the database and then an RPC sent to the pageserver in question to synchronize
it.

When creating tenants, a warm secondary location may be selected using the same
logic as picking the pageserver to attach to, and the attached pageserver should
also be set as a warm secondary.

When detaching tenants the control plane may indicate inline to the
pageserver whether the tenant should be retained as a warm location, or
completely removed, depending on whether the detaching pageserver still appears
in the list of locations in the control plane database.

### Reliability, failure modes and corner cases (if relevant)

### Interaction/Sequence diagram (if relevant)

### Scalability (if relevant)

### Security implications (if relevant)

### Unresolved questions (if relevant)

## Alternative implementation (if relevant)

## Pros/cons of proposed approaches (if relevant)

## Definition of Done (if relevant)

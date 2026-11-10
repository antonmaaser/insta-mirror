# Engineering principles

This repository is an Instagram Direct Message mirror, not a messaging client or a Matrix bridge. Its purpose is to acquire source data reliably, preserve its identity and provenance, and expose locally held information through read-only MCP queries. Favor a small, maintainable Go program over a generalized integration platform.

## Work with the upstream protocol, not around it

Treat `instameow`, its `slidetypes` definitions, and the genuinely required shared Meta protocol packages as the authority for authentication, transport, and source parsing. Read the relevant call path before editing it. Prefer narrow integration points and additive hooks to copied protocol logic or broad refactors. Keep protocol changes easy to compare and rebase against upstream.

Make changes at the closest appropriate boundary: protocol packages acquire and decode; ingestion identifies and persists; storage retrieves; projections interpret; MCP presents. Do not let Matrix-specific types, MCP output formats, or database implementation details leak back into the Instagram transport.

## Model the source, not a second messaging system

SQLite reflects Instagram source entities as immutable normalized versions, including threads, messages, users, realtime deltas, and their nested entities. Synchronization state is operational metadata. Store acquisition provenance separately from shared content versions.

Normalize nested content into related source versions. Ordered relationships retain source field locations, array positions, repeated members, and the exact historical child versions. Scalar residuals and relationships together must reconstruct every source field and value without retaining a duplicate original payload. Preserve unknown fields, duplicate object keys, exact numbers, JSON types, and missing/null/empty distinctions. Use SQLite JSON facilities and indexes for scalar source fields rather than duplicate mutable values. Preserve identifiers in their original namespaces; never use usernames or display labels as identity keys.

Normalize captured source JSON before typed decoding can discard unknown data. Reconstruct lossless JSON content, not necessarily its original whitespace, object-key order, escape spelling, or number spelling. Keep original acquisition byte hashes distinct from versioned normalized-content fingerprints. Verify content equality before sharing a fingerprint match. Treat snapshots as observations, not permanent claims about current state; retain distinguishable versions and deltas for future interpretation. Incompatible databases are archived operationally; do not add migration, downgrade, or legacy-reader machinery.

## Put interpretation at the query boundary

Ingestion should perform only the interpretation needed to classify a source object, identify it, relate it to its account or thread, and commit it safely. The existing `slidetypes` structures are the preferred typed vocabulary. Build current-message views, participant summaries, media descriptions, and search responses just in time from reconstructed source JSON and relevant changes.

Do not persist an additional canonical message model or create parallel types that merely rename upstream fields. Derived indexes and media caches are disposable accelerators; the normalized source versions, relationships, and acquisition records remain authoritative. When source ordering, identity, or availability cannot be established, represent the uncertainty instead of inventing certainty.

## Synchronization is a consistency problem

Inbox discovery, per-thread history pagination, and realtime updates are different streams of evidence. Keep their progress and coverage separate. Recording an observation and advancing its checkpoint must be crash-safe; restarting or encountering overlapping pages must not lose or multiply source events. A failed or unknown delta must remain observable and recoverable rather than silently disappearing.

Respect distinctions between event sequence, source timestamp, and local observation time. Reconciliation must account for snapshots that already incorporate earlier changes. Inaccessible, expired, or deleted source content is not proof of an empty conversation; report the boundary of what was actually acquired.

## Keep the interface narrower than the archive

MCP is a read-only view into locally mirrored data, not a direct proxy to Instagram actions. Prefer a few predictable, bounded, account-scoped retrieval operations with stable source references. Responses should label reconstructed source content and distinguish it from interpreted current state and include enough identifiers to trace a result back to its stored record.

Protect session credentials and private conversations as sensitive local data. Never mix authentication secrets into archived payloads or diagnostic output. Keep network exposure opt-in and do not contaminate stdio MCP output with logs.

## What belongs in this repository

Produce idiomatic, direct Go code with explicit errors, clear ownership of resources, and minimal dependencies. Prefer small cohesive functions and packages over frameworks, registries, generic abstraction layers, or speculative extensibility. Keep only executable functionality, required schema and build assets, necessary dependencies, and licensing notices. Remove obsolete bridge-specific code once it is genuinely unreachable, without rewriting working protocol components for cosmetic purity.

A change is good when it preserves more source truth, improves recoverability, reduces moving parts, or makes the observable behavior clearer. A change that only adds structure, output, or configuration without serving those ends should not be made.

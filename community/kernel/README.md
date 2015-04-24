# Neo4j Kernel

This module, for historical reasons, contains multiple important components of Neo4j:

 - The embedded Java API
    - org.neo4j.graphdb
 - The embedded Java API implementation
    - org.neo4j.kernel.coreapi
    - org.neo4j.kernel.core
 - The embedded Traversal Java API
   - org.neo4j.graphdb.traversal
 - The embedded Traversal API implementation
    - org.neo4j.kernel.traversal
 - Batch Import
    - org.neo4j.unsafe.impl.batchimport
 - Batch Inserter (legacy)
    - org.neo4j.unsafe.batchinsert
 - The transaction state building layer (the "Kernel API")
    - org.neo4j.kernel.api
    - org.neo4j.kernel.impl.api
 - The Storage Engine:
    - org.neo4j.kernel.impl.store,
    - org.neo4j.kernel.impl.recovery
    - org.neo4j.kernel.impl.transaction
 - Configuration
    - org.neo4j.kernel.configuration
 - Common utilities
    - org.neo4j.helpers
    - org.neo4j.kernel.impl.util
    - org.neo4j.kernel.lifecycle
    - org.neo4j.kernel.monitoring
 - Locking
    - org.neo4j.kernel.impl.locking
 - Kernel Extensions
    - org.neo4j.kernel.extension

package org.neo4j.kernel.ha;

import org.neo4j.graphdb.PropertyContainer;

public interface PulledOutHooksFromEmbeddedGraphDb
{
    void createRelationshipType( String name );

    void acquireWriteLock( PropertyContainer entity );

    void commitTx();

    void rollbackTx();
}

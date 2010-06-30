package org.neo4j.graphdb.index;

import java.util.Map;

public interface BatchInserterIndexProvider
{
    BatchInserterIndex nodeIndex( String indexName, Map<String, String> config );
    
    BatchInserterIndex relationshipIndex( String indexName, Map<String, String> config );
    
    void shutdown();
}

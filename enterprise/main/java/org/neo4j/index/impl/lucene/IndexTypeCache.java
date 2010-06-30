package org.neo4j.index.impl.lucene;

import java.util.HashMap;
import java.util.Map;

class IndexTypeCache
{
    private final Map<String, IndexType> cache = new HashMap<String, IndexType>();
    
    IndexType getIndexType( IndexIdentifier identifier )
    {
        IndexType type = cache.get( identifier.indexName );
        if ( type != null )
        {
            return type;
        }
        type = IndexType.getIndexType( identifier );
        cache.put( identifier.indexName, type );
        return type;
    }
}

package org.neo4j.index.impl.lucene;

import java.util.Map;

class IndexIdentifier
{
    final String indexName;
    final Map<String, String> config;
    final EntityType entityType;
    
    public IndexIdentifier( EntityType entityType, String indexName,
            Map<String, String> customConfig )
    {
        this.entityType = entityType;
        this.indexName = indexName;
        this.config = customConfig;
    }
    
    @Override
    public boolean equals( Object o )
    {
        if ( o == null || !getClass().equals( o.getClass() ) )
        {
            return false;
        }
        IndexIdentifier i = (IndexIdentifier) o;
        return entityType.getType().equals( i.entityType.getType() ) &&
                indexName.equals( i.indexName );
    }
    
    @Override
    public int hashCode()
    {
        int code = 17;
        code += 7*entityType.getType().hashCode();
        code += 7*indexName.hashCode();
        return code;
    }
}

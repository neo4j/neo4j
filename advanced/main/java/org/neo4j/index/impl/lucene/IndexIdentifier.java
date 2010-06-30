package org.neo4j.index.impl.lucene;

import java.util.Map;

class IndexIdentifier
{
    final Class<?> itemsClass;
    final String indexName;
    final Map<String, String> config;
    
    public IndexIdentifier( Class<?> itemsClass, String indexName,
            Map<String, String> customConfig )
    {
        this.itemsClass = itemsClass;
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
        return itemsClass.equals( i.itemsClass ) &&
                indexName.equals( i.indexName );
    }
    
    @Override
    public int hashCode()
    {
        int code = 17;
        code += 7*itemsClass.hashCode();
        code += 7*indexName.hashCode();
        return code;
    }
}

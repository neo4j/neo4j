package org.neo4j.index.impl.lucene;

import org.neo4j.graphdb.Node;
import org.neo4j.index.ReadOnlyIndexException;

class LuceneReadOnlyIndexServiceWrapper extends LuceneIndexServiceWrapper
{
    public LuceneReadOnlyIndexServiceWrapper( LuceneIndexProvider provider )
    {
        super( provider );
    }
    
    @Override
    public void index( Node node, String key, Object value )
    {
        throw new ReadOnlyIndexException();
    }
    
    @Override
    public void removeIndex( Node node, String key, Object value )
    {
        throw new ReadOnlyIndexException();
    }
    
    @Override
    public void removeIndex( Node node, String key )
    {
        throw new ReadOnlyIndexException();
    }
    
    @Override
    public void removeIndex( String key )
    {
        throw new ReadOnlyIndexException();
    }
}

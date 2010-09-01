package org.neo4j.index.impl.lucene;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.IndexHits;

class LuceneFulltextQueryIndexServiceWrapper extends LuceneFulltextIndexServiceWrapper
{
    public LuceneFulltextQueryIndexServiceWrapper( LuceneIndexProvider provuder )
    {
        super( provuder );
    }

    @Override
    public IndexHits<Node> getNodes( String key, Object value )
    {
        return getIndex( key ).query( key, value );
    }
}

package org.neo4j.kernel.impl.api.index;

import org.neo4j.kernel.impl.nioneo.store.IndexRule;

public class WritingIndexContext implements IndexContext
{
    private final IndexWriter writer;
    private final IndexRule indexRule;

    public WritingIndexContext( IndexWriter writer, IndexRule indexRule )
    {
        this.writer = writer;
        this.indexRule = indexRule;
    }
    
    @Override
    public void create()
    {
        writer.createIndex();
    }

    @Override
    public void ready()
    {
    }

    @Override
    public void update( Iterable<NodePropertyUpdate> updates )
    {
        for ( NodePropertyUpdate update : updates )
            if ( (update.getPropertyKeyId() == indexRule.getPropertyKey()) && (update.hasLabel( indexRule.getLabel() )) )
                update.apply( writer );
    }

    @Override
    public void drop()
    {
        writer.dropIndex();
    }

    @Override
    public IndexRule getIndexRule()
    {
        return indexRule;
    }
}

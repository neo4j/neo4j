package org.neo4j.kernel.impl.api.index;

public class OnlineIndexContext implements IndexContext
{
    private final IndexWriter writer;

    public OnlineIndexContext( IndexWriter writer )
    {
        this.writer = writer;
    }
    
    @Override
    public void create()
    {
        throw new UnsupportedOperationException(  );
    }

    @Override
    public void update( Iterable<NodePropertyUpdate> updates )
    {
        // We assume updates have already been filtered here
        for ( NodePropertyUpdate update : updates )
            update.apply( writer );
    }

    @Override
    public void drop()
    {
        writer.dropIndex();
    }
}

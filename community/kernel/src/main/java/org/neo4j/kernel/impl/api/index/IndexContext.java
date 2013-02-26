package org.neo4j.kernel.impl.api.index;

import org.neo4j.kernel.impl.nioneo.store.IndexRule;

public interface IndexContext
{
    void create();
    
    void ready();
    
    void update( Iterable<NodePropertyUpdate> updates );
    
    void drop();
    
    IndexRule getIndexRule();
    
    public static abstract class Adapter implements IndexContext
    {
        public static final Adapter EMPTY = new Adapter()
        {
            @Override
            public IndexRule getIndexRule()
            {
                throw new UnsupportedOperationException(  );
            }
        };

        @Override
        public void create()
        {
        }

        @Override
        public void ready()
        {
        }

        @Override
        public void update( Iterable<NodePropertyUpdate> updates )
        {
        }

        @Override
        public void drop()
        {
        }
    }
}

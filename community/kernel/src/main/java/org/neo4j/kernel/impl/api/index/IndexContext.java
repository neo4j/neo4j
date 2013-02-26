package org.neo4j.kernel.impl.api.index;

public interface IndexContext
{
    void create();
    
    void update( Iterable<NodePropertyUpdate> updates );
    
    void drop();
    
    public static class Adapter implements IndexContext
    {
        public static final Adapter EMPTY = new Adapter();

        @Override
        public void create()
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

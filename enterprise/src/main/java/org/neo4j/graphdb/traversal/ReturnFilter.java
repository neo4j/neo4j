package org.neo4j.graphdb.traversal;

public interface ReturnFilter
{
    static final ReturnFilter ALL = new ReturnFilter()
    {
        public boolean shouldReturn( Position position )
        {
            return true;
        }
    };
    
    static final ReturnFilter ALL_BUT_START_NODE = new ReturnFilter()
    {
        public boolean shouldReturn( Position position )
        {
            return !position.atStartNode();
        }
    };

    boolean shouldReturn( Position position );
}

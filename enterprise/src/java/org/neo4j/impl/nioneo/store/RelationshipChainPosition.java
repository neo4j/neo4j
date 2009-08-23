package org.neo4j.impl.nioneo.store;

public class RelationshipChainPosition
{
    private int nextRecord;
    
    public RelationshipChainPosition( int startRecord )
    {
        nextRecord = startRecord;
    }
    
    public int getNextRecord()
    {
        return nextRecord;
    }
    
    public void setNextRecord( int record )
    {
        nextRecord = record;
    }

    public boolean hasMore()
    {
        return nextRecord != Record.NO_NEXT_RELATIONSHIP.intValue();
    }
}

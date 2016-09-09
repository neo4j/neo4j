package org.neo4j.index;

public class SCValue
{
    private long relId;
    private long nodeId;

    public SCValue( long relId, long nodeId )
    {
        this.relId = relId;
        this.nodeId = nodeId;
    }

    public long getRelId()
    {
        return relId;
    }

    public long getNodeId()
    {
        return nodeId;
    }

    @Override
    public int hashCode() {
        return (int) (relId * 23 + nodeId);
    }

    @Override
    public boolean equals( Object obj ) {
        if ( !( obj instanceof SCValue) )
            return false;
        if ( obj == this )
            return true;

        SCValue rhs = (SCValue) obj;
        return relId == rhs.relId && nodeId == rhs.nodeId;
    }

    @Override
    public String toString()
    {
        return String.format( "(%d,%d)", relId, nodeId );
    }
}

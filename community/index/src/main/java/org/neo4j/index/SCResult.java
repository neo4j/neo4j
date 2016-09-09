package org.neo4j.index;

public class SCResult
{
    private final SCKey key;
    private final SCValue value;

    public SCResult( SCKey key, SCValue value )
    {
        this.key = key;
        this.value = value;
    }

    public SCKey getKey()
    {
        return key;
    }

    public SCValue getValue()
    {
        return value;
    }

    @Override
    public int hashCode()
    {
        return key.hashCode() * 23 + value.hashCode();
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( !( obj instanceof SCResult) )
            return false;
        if ( obj == this )
            return true;

        SCResult rhs = (SCResult) obj;
        return this.getKey().equals( rhs.getKey() ) && getValue().equals( rhs.getValue() );
    }

    @Override
    public String toString()
    {
        return String.format( "(%d,%d):(%d,%d)", key.getId(), key.getProp(), value.getRelId(), value.getNodeId() );
    }
}

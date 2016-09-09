package org.neo4j.index;

import java.util.Objects;

public class SCKey implements Comparable<SCKey>
{
    private long id;
    private long prop;

    public SCKey( long id, long prop )
    {
        this.id = id;
        this.prop = prop;
    }

    public long getId()
    {
        return id;
    }

    public long getProp()
    {
        return prop;
    }

    @Override
    public int compareTo( SCKey o )
    {
        Objects.requireNonNull( o );
        return id == o.id ? Long.compare( prop, o.prop ) : Long.compare( id, o.id );
    }

    @Override
    public int hashCode() {
        return (int) ( id * 23 + prop );
    }

    @Override
    public boolean equals( Object obj ) {
        if ( !( obj instanceof SCKey) )
            return false;
        if ( obj == this )
            return true;

        SCKey rhs = (SCKey) obj;
        return this.compareTo( rhs ) == 0;
    }

    @Override
    public String toString()
    {
        return String.format( "(%d,%d)", id, prop );
    }

}

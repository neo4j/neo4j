package org.neo4j.cypher.internal.compiler.v2_3.codegen;

public final class CompiledRelationship
{
    private final long id;

    public CompiledRelationship( long id )
    {
        this.id = id;
    }

    public long id()
    {
        return id;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        CompiledRelationship that = (CompiledRelationship) o;

        return id == that.id;

    }

    @Override
    public int hashCode()
    {
        return (int) (id ^ (id >>> 32));
    }
}

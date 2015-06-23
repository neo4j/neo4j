package org.neo4j.cypher.internal.compiler.v2_3.codegen;

public final class CompiledNode
{
    private final long id;

    public CompiledNode( long id )
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

        CompiledNode that = (CompiledNode) o;

        return id == that.id;

    }

    @Override
    public int hashCode()
    {
        return (int) (id ^ (id >>> 32));
    }
}

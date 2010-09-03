package org.neo4j.helpers;

public class Triplet<T1, T2, T3> extends Pair<T1, T2>
{
    private final T3 third;

    public Triplet( T1 first, T2 other, T3 third )
    {
        super( first, other );
        this.third = third;
    }
    
    public T3 third()
    {
        return this.third;
    }
    
    @Override
    public String toString()
    {
        return "(" + first() + ", " + other() + ", " + third + ")";
    }
}

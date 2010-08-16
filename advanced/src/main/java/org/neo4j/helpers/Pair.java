package org.neo4j.helpers;

/**
 * Utility to handle pairs of objects.
 */
public final class Pair<T1, T2>
{
    private final T1 first;
    private final T2 other;

    public Pair( T1 first, T2 other )
    {
        this.first = first;
        this.other = other;
    }

    public T1 first()
    {
        return first;
    }

    public T2 other()
    {
        return other;
    }
    
    @Override
    public String toString()
    {
        return "(" + first + ", " + other + ")";
    }
}

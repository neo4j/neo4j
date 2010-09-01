package org.neo4j.helpers;

/**
 * Utility to handle pairs of objects.
 */
public final class Pair<T1, T2>
{
    private final T1 first;
    private final T2 other;

    /**
     * Create a new pair of objects.
     * 
     * @param first the first object in the pair.
     * @param other the other object in the pair.
     */
    public Pair( T1 first, T2 other )
    {
        this.first = first;
        this.other = other;
    }

    /**
     * @return the first object in the pair.
     */
    public T1 first()
    {
        return first;
    }

    /**
     * @return the other object in the pair.
     */
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

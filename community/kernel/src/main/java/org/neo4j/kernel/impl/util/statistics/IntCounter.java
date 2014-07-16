package org.neo4j.kernel.impl.util.statistics;

/**
 * A wrapper for a primitive int counter, allowing it to be passed around different components.
 */
public class IntCounter
{
    private int count = 0;

    public int value()
    {
        return count;
    }

    public void increment()
    {
        count++;
    }

    public void decrement()
    {
        count--;
    }
}

package org.neo4j.helpers.collection;

/**
 * Iterates over a range, where the start value is inclusive, but the
 * end value is exclusive.
 */
public class RangeIterator extends PrefetchingIterator<Integer>
{
    private int current;
    private final int end;
    private final int stride;
    
    public RangeIterator( int end )
    {
        this( 0, end );
    }
    
    public RangeIterator( int start, int end )
    {
        this( start, end, 1 );
    }
    
    public RangeIterator( int start, int end, int stride )
    {
        this.current = start;
        this.end = end;
        this.stride = stride;
    }
    
    @Override
    protected Integer fetchNextOrNull()
    {
        try
        {
            return current < end ? current : null;
        }
        finally
        {
            current += stride;
        }
    }
}

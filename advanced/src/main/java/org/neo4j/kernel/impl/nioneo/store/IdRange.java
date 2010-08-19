package org.neo4j.kernel.impl.nioneo.store;

public class IdRange
{
    private final long[] defragIds;
    private final long rangeStart;
    private final int rangeLength;

    public IdRange( long[] defragIds, long rangeStart, int rangeLength )
    {
        this.defragIds = defragIds;
        this.rangeStart = rangeStart;
        this.rangeLength = rangeLength;
    }

    public long[] getDefragIds()
    {
        return defragIds;
    }

    public long getRangeStart()
    {
        return rangeStart;
    }

    public int getRangeLength()
    {
        return rangeLength;
    }
}

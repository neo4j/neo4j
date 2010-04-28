package org.neo4j.kernel.impl.nioneo.store;

public class WindowPoolStats
{
    private final String name;
    
    private final long memAvail;
    private final long memUsed;
    
    private final int windowCount;
    private final int windowSize;
    
    private final int hitCount;
    private final int missCount;
    private final int oomCount;
    
    public WindowPoolStats( String name, long memAvail, long memUsed, int windowCount,
            int windowSize, int hitCount, int missCount, int oomCount )
    {
        this.name = name;
        this.memAvail = memAvail;
        this.memUsed = memUsed;
        this.windowCount = windowCount;
        this.windowSize = windowSize;
        this.hitCount = hitCount;
        this.missCount = missCount;
        this.oomCount = oomCount;
    }
    
    public String getName()
    {
        return name;
    }

    public long getMemAvail()
    {
        return memAvail;
    }

    public long getMemUsed()
    {
        return memUsed;
    }

    public int getWindowCount()
    {
        return windowCount;
    }

    public int getWindowSize()
    {
        return windowSize;
    }

    public int getHitCount()
    {
        return hitCount;
    }

    public int getMissCount()
    {
        return missCount;
    }

    public int getOomCount()
    {
        return oomCount;
    }
}

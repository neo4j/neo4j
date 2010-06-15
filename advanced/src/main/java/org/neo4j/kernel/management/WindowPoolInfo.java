package org.neo4j.kernel.management;

import java.io.Serializable;

import org.neo4j.kernel.impl.nioneo.store.WindowPoolStats;

public final class WindowPoolInfo implements Serializable
{
    private static final long serialVersionUID = 1L;
    private final String name;
    private final long memAvail;
    private final long memUsed;
    private final int windowCount;
    private final int windowSize;
    private final int hitCount;
    private final int missCount;
    private final int oomCount;

    /* Java 1.6 specific
    @ConstructorProperties( { "windowPoolName", "availableMemory",
            "usedMemory", "numberOfWindows", "windowSize", "windowHitCount",
            "windowMissCount", "numberOfOutOfMemory" } )
    */
    public WindowPoolInfo( String name, long memAvail, long memUsed,
            int windowCount, int windowSize, int hitCount, int missCount,
            int oomCount )
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

    WindowPoolInfo( WindowPoolStats stats )
    {
        this( stats.getName(), stats.getMemAvail(), stats.getMemUsed(),
                stats.getWindowCount(), stats.getWindowSize(),
                stats.getHitCount(), stats.getMissCount(), stats.getOomCount() );
    }

    public String getWindowPoolName()
    {
        return name;
    }

    public long getAvailableMemory()
    {
        return memAvail;
    }

    public long getUsedMemory()
    {
        return memUsed;
    }

    public int getNumberOfWindows()
    {
        return windowCount;
    }

    public int getWindowSize()
    {
        return windowSize;
    }

    public int getWindowHitCount()
    {
        return hitCount;
    }

    public int getWindowMissCount()
    {
        return missCount;
    }

    public int getNumberOfOutOfMemory()
    {
        return oomCount;
    }
}

/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.nioneo.store;

import java.io.File;

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

    private final int switchCount;
    private final int avgRefreshTime;
    private final int refreshCount;
    private final int avertedRefreshCount;
    
    public WindowPoolStats( File file, long memAvail, long memUsed, int windowCount,
            int windowSize, int hitCount, int missCount, int oomCount, int switchCount, int avgRefreshTime,
            int refreshCount, int avertedRefreshCount )
    {
        this.name = file.getName();
        this.memAvail = memAvail;
        this.memUsed = memUsed;
        this.windowCount = windowCount;
        this.windowSize = windowSize;
        this.hitCount = hitCount;
        this.missCount = missCount;
        this.oomCount = oomCount;
        this.switchCount = switchCount;
        this.avgRefreshTime = avgRefreshTime;
        this.refreshCount = refreshCount;
        this.avertedRefreshCount = avertedRefreshCount;
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
    
    public int getSwitchCount()
    {
        return switchCount;
    }
    
    public int getAvgRefreshTime()
    {
        return avgRefreshTime;
    }
    
    public int getRefreshCount()
    {
        return refreshCount;
    }
    
    public int getAvertedRefreshCount()
    {
        return avertedRefreshCount;
    }
    
    @Override
    public String toString()
    {
        return "WindowPoolStats['" + name + "', " +
                "memAvail:" + memAvail + ", " +
                "memUsed:" + memUsed + ", " +
                "windowCount:" + windowCount + ", " +
                "windowSize:" + windowSize + ", " +
                "hitCount:" + hitCount + ", " +
                "missCount:" + missCount + ", " +
                "oomCount:" + oomCount + ", " +
                "switchCount:" + switchCount + ", " +
                "avgRefreshTime:" + avgRefreshTime + ", " +
                "refreshCount:" + refreshCount + ", " +
                "avertedRefreshCount:" + avertedRefreshCount +
                "]";
    }
}

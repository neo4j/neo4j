/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.management;

import java.beans.ConstructorProperties;
import java.io.Serializable;

public final class WindowPoolInfo implements Serializable
{
    private static final long serialVersionUID = 7743724554758487292L;

    private String name;
    private long memAvail;
    private long memUsed;
    private int windowCount;
    private int windowSize;
    private int hitCount;
    private int missCount;
    private int oomCount;

    @ConstructorProperties( { "windowPoolName", "availableMemory",
            "usedMemory", "numberOfWindows", "windowSize", "windowHitCount",
            "windowMissCount", "numberOfOutOfMemory" } )
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

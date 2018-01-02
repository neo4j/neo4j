/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.management;

import java.beans.ConstructorProperties;
import java.io.Serializable;

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

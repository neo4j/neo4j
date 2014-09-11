/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.io.pagecache.stress;

import static java.lang.String.format;

import org.neo4j.io.pagecache.PageCacheMonitor;
import org.neo4j.io.pagecache.PageSwapper;

public class SimpleMonitor implements PageCacheMonitor
{
    private long faults;
    private long evictions;
    private long pins;
    private long unpins;
    private long flushes;

    @Override
    public void pageFaulted( long filePageId, PageSwapper swapper )
    {
        faults++;
    }

    @Override
    public void evicted( long filePageId, PageSwapper swapper )
    {
        evictions++;
    }

    @Override
    public void pinned( boolean exclusiveLock, long filePageId, PageSwapper swapper )
    {
        pins++;
    }

    @Override
    public void unpinned( boolean exclusiveLock, long filePageId, PageSwapper swapper )
    {
        unpins++;
    }

    @Override
    public void flushed( long filePageId, PageSwapper swapper )
    {
        flushes++;
    }

    public long getNumberOfEvictions()
    {
        return evictions;
    }

    @Override
    public String toString()
    {
        return format("Monitoring:%n - page faults: %d%n - evictions: %d%n - pins: %d%n - unpins: %d%n - flushes: %d", faults, evictions, pins, unpins, flushes);
    }
}

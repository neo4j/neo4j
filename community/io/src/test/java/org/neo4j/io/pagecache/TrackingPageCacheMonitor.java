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
package org.neo4j.io.pagecache;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

class TrackingPageCacheMonitor implements PageCacheMonitor
{
    Queue<String> queue = new ConcurrentLinkedQueue<>();

    @Override
    public void pageFault( long filePageId, PageSwapper swapper )
    {
        queue.offer( threadId() + "fault(" + filePageId + ", " + swapper + ")" );
    }

    private String threadId()
    {
        return Thread.currentThread().getId() + ": ";
    }

    @Override
    public void evict( long filePageId, PageSwapper swapper )
    {
        queue.offer( threadId() + "evict(" + filePageId + ", " + swapper + ")" );
    }

    @Override
    public void pin( boolean exclusiveLock, long filePageId, PageSwapper swapper )
    {
        queue.offer( threadId() + "pin(" + exclusiveLock + ", " + filePageId + ", " + swapper + ")" );
    }

    @Override
    public void unpin( boolean exclusiveLock, long filePageId, PageSwapper swapper )
    {
        queue.offer( threadId() + "unpin(" + exclusiveLock + ", " + filePageId + ", " + swapper + ")" );
    }

    @Override
    public void flush( long filePageId, PageSwapper swapper )
    {
        queue.offer( threadId() + "flush(" + filePageId + ", " + swapper + ")" );
    }
}

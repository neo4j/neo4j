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
package org.neo4j.io.pagecache.monitoring.jfr;

import com.oracle.jrockit.jfr.EventDefinition;
import com.oracle.jrockit.jfr.TimedEvent;
import com.oracle.jrockit.jfr.ValueDefinition;

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.monitoring.FlushEvent;
import org.neo4j.io.pagecache.monitoring.FlushEventOpportunity;
import org.neo4j.io.pagecache.monitoring.MajorFlushEvent;

@EventDefinition(path = "neo4j/io/pagecache/cacheflush")
public class JfrCacheFlushEvent extends TimedEvent implements MajorFlushEvent, FlushEventOpportunity
{
    private static final AtomicLong cacheFlushCounter = new AtomicLong();
    static final String REL_KEY_CACHE_FLUSH_ID = "http://neo4j.com/jfr/cacheFlushId";

    private final AtomicLong flushes;
    private final AtomicLong bytesWritten;

    @ValueDefinition(name = "cacheFlushEventId", relationKey = REL_KEY_CACHE_FLUSH_ID)
    private long cacheFlushEventId;

    public JfrCacheFlushEvent( AtomicLong flushes, AtomicLong bytesWritten )
    {
        super( JfrPageCacheMonitor.cacheFlushToken );
        this.flushes = flushes;
        this.bytesWritten = bytesWritten;
        cacheFlushEventId = cacheFlushCounter.incrementAndGet();
    }

    @Override
    public FlushEventOpportunity flushEventOpportunity()
    {
        return this;
    }

    @Override
    public void close()
    {
        end();
        commit();
    }

    @Override
    public FlushEvent beginFlush( long filePageId, int cachePageId, PageSwapper swapper )
    {
        flushes.getAndIncrement();
        JfrFlushEvent event = new JfrFlushEvent( bytesWritten );
        event.begin();
        event.setFilePageId( filePageId );
        event.setCachePageId( cachePageId );
        event.setSwapper( swapper );
        event.setCacheFlushEventId( cacheFlushEventId );
        return event;
    }

    public long getCacheFlushEventId()
    {
        return cacheFlushEventId;
    }

    public void setCacheFlushEventId( long cacheFlushEventId )
    {
        this.cacheFlushEventId = cacheFlushEventId;
    }
}

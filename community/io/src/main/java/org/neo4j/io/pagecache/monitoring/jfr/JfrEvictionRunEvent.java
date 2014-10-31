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

import org.neo4j.io.pagecache.monitoring.EvictionEvent;
import org.neo4j.io.pagecache.monitoring.EvictionRunEvent;

@EventDefinition(path = "neo4j/io/pagecache/evictionrun")
public class JfrEvictionRunEvent extends TimedEvent implements EvictionRunEvent
{
    static final String REL_KEY_EVICTION_RUN = "http://neo4j.com/jfr/evictionRun";
    private final AtomicLong evictionsCounter;
    private final AtomicLong evictionExceptions;
    private final AtomicLong flushes;
    private final AtomicLong bytesWritten;

    @ValueDefinition(name = "evictionRun", relationKey = REL_KEY_EVICTION_RUN )
    private long evictionRun;
    @ValueDefinition(name = "expectedEvictions")
    private int expectedEvictions;
    @ValueDefinition(name = "actualEvictions")
    private int actualEvictions;

    public JfrEvictionRunEvent(
            AtomicLong evictionsCounter,
            AtomicLong evictionExceptions,
            AtomicLong flushes,
            AtomicLong bytesWritten )
    {
        super( JfrPageCacheMonitor.evictionRunToken );
        this.evictionsCounter = evictionsCounter;
        this.evictionExceptions = evictionExceptions;
        this.flushes = flushes;
        this.bytesWritten = bytesWritten;
    }

    @Override
    public EvictionEvent beginEviction()
    {
        actualEvictions++;
        long evictionId = evictionsCounter.incrementAndGet();
        JfrEvictionEvent evictionEvent = new JfrEvictionEvent(
                evictionExceptions, flushes, bytesWritten );
        evictionEvent.begin();
        evictionEvent.setEvictionRun( evictionRun );
        evictionEvent.setEvictionId( evictionId );
        return evictionEvent;
    }

    @Override
    public void close()
    {
        end();
        commit();
    }

    public void setExpectedEvictions( int expectedEvictions )
    {
        this.expectedEvictions = expectedEvictions;
    }

    public int getExpectedEvictions()
    {
        return expectedEvictions;
    }

    public void setEvictionRun( long evictionRun )
    {
        this.evictionRun = evictionRun;
    }

    public long getEvictionRun()
    {
        return evictionRun;
    }

    public int getActualEvictions()
    {
        return actualEvictions;
    }

    public void setActualEvictions( int actualEvictions )
    {
        this.actualEvictions = actualEvictions;
    }
}

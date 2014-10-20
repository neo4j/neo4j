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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.monitoring.EvictionEvent;
import org.neo4j.io.pagecache.monitoring.FlushEvent;
import org.neo4j.io.pagecache.monitoring.FlushEventOpportunity;

@EventDefinition(path = "neo4j/io/pagecache/eviction")
public class JfrEvictionEvent extends TimedEvent implements EvictionEvent, FlushEventOpportunity
{
    static final String REL_KEY_EVICTION_ID = "http://neo4j.com/jfr/evictionId";
    private final AtomicLong evictionExceptions;
    private final AtomicLong flushes;
    private final AtomicLong bytesWritten;

    @ValueDefinition(name = "evictionRun", relationKey = JfrEvictionRunEvent.REL_KEY_EVICTION_RUN )
    private long evictionRun;
    @ValueDefinition(name = "evictionId", relationKey = REL_KEY_EVICTION_ID)
    private long evictionId;
    @ValueDefinition(name = "filePageId")
    private long filePageId;
    @ValueDefinition(name = "filename")
    private String filename;
    @ValueDefinition(name = "gotException")
    private boolean gotException;
    @ValueDefinition(name = "exceptionMessage")
    private String exceptionMessage;
    @ValueDefinition(name = "cachePageId")
    private int cachePageId;
    @ValueDefinition(name = "causedFlush")
    private boolean causedFlush;

    public JfrEvictionEvent(
            AtomicLong evictionExceptions,
            AtomicLong flushes,
            AtomicLong bytesWritten )
    {
        super( JfrPageCacheMonitor.evictionToken );
        this.evictionExceptions = evictionExceptions;
        this.flushes = flushes;
        this.bytesWritten = bytesWritten;
    }

    @Override
    public void setFilePageId( long filePageId )
    {
        this.filePageId = filePageId;
    }

    @Override
    public void setSwapper( PageSwapper swapper )
    {
        this.filename = swapper.fileName();
    }

    @Override
    public FlushEventOpportunity flushEventOpportunity()
    {
        return this;
    }

    @Override
    public FlushEvent beginFlush( long filePageId, int cachePageId, PageSwapper swapper )
    {
        flushes.getAndIncrement();
        causedFlush = true;
        JfrFlushEvent event = new JfrFlushEvent( bytesWritten );
        event.begin();
        event.setFilePageId( filePageId );
        event.setCachePageId( cachePageId );
        event.setSwapper( swapper );
        event.setEvictionId( evictionId );
        return event;
    }

    @Override
    public void threwException( IOException exception )
    {
        this.gotException = true;
        this.exceptionMessage = exception.getMessage();
        evictionExceptions.getAndIncrement();
    }

    @Override
    public void setCachePageId( int cachePageId )
    {
        this.cachePageId = cachePageId;
    }

    @Override
    public void close()
    {
        end();
        commit();
    }

    public void setEvictionRun( long evictionRun )
    {
        this.evictionRun = evictionRun;
    }

    public void setEvictionId( long evictionId )
    {
        this.evictionId = evictionId;
    }

    public int getCachePageId()
    {
        return cachePageId;
    }

    public String getExceptionMessage()
    {
        return exceptionMessage;
    }

    public boolean getGotException()
    {
        return gotException;
    }

    public String getFilename()
    {
        return filename;
    }

    public long getFilePageId()
    {
        return filePageId;
    }

    public long getEvictionRun()
    {
        return evictionRun;
    }

    public long getEvictionId()
    {
        return evictionId;
    }

    public boolean getCausedFlush()
    {
        return causedFlush;
    }
}

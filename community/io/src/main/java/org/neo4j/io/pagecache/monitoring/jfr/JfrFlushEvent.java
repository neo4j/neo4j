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

import com.oracle.jrockit.jfr.ContentType;
import com.oracle.jrockit.jfr.EventDefinition;
import com.oracle.jrockit.jfr.TimedEvent;
import com.oracle.jrockit.jfr.ValueDefinition;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.monitoring.FlushEvent;

@EventDefinition(path = "neo4j/io/pagecache/flush")
public class JfrFlushEvent extends TimedEvent implements FlushEvent
{
    private final AtomicLong bytesWrittenTotal;

    @ValueDefinition(name = "filePageId")
    private long filePageId;
    @ValueDefinition(name = "cachePageId")
    private int cachePageId;
    @ValueDefinition(name = "filename")
    private String filename;
    @ValueDefinition(name = "bytesWritten", contentType = ContentType.Bytes)
    private int bytesWritten;
    @ValueDefinition(name = "gotException")
    private boolean gotException;
    @ValueDefinition(name = "exceptionMessage")
    private String exceptionMessage;
    @ValueDefinition(name = "evictionId", relationKey = JfrEvictionEvent.REL_KEY_EVICTION_ID)
    private long evictionId;
    @ValueDefinition(name = "fileFlushEventId", relationKey = JfrFileFlushEvent.REL_KEY_FILE_FLUSH_ID)
    private long fileFlushEventId;
    @ValueDefinition(name = "cacheFlushEventId", relationKey = JfrCacheFlushEvent.REL_KEY_CACHE_FLUSH_ID)
    private long cacheFlushEventId;

    public JfrFlushEvent( AtomicLong bytesWritten )
    {
        super( JfrPageCacheMonitor.flushToken );
        bytesWrittenTotal = bytesWritten;
    }

    @Override
    public void addBytesWritten( int bytes )
    {
        this.bytesWritten += bytes;
        bytesWrittenTotal.getAndAdd( bytes );
    }

    @Override
    public void done()
    {
        end();
        commit();
    }

    @Override
    public void done( IOException exception )
    {
        this.gotException = true;
        this.exceptionMessage = exception.getMessage();
        done();
    }

    public void setFilePageId( long filePageId )
    {
        this.filePageId = filePageId;
    }

    public void setCachePageId( int cachePageId )
    {
        this.cachePageId = cachePageId;
    }

    public void setSwapper( PageSwapper swapper )
    {
        this.filename = swapper.fileName();
    }

    public void setEvictionId( long evictionId )
    {
        this.evictionId = evictionId;
    }

    public void setFileFlushEventId( long fileFlushEventId )
    {
        this.fileFlushEventId = fileFlushEventId;
    }

    public void setCacheFlushEventId( long cacheFlushEventId )
    {
        this.cacheFlushEventId = cacheFlushEventId;
    }

    public long getFilePageId()
    {
        return filePageId;
    }

    public int getCachePageId()
    {
        return cachePageId;
    }

    public String getFilename()
    {
        return filename;
    }

    public int getBytesWritten()
    {
        return bytesWritten;
    }

    public boolean getGotException()
    {
        return gotException;
    }

    public String getExceptionMessage()
    {
        return exceptionMessage;
    }

    public long getEvictionId()
    {
        return evictionId;
    }

    public long getFileFlushEventId()
    {
        return fileFlushEventId;
    }

    public long getCacheFlushEventId()
    {
        return cacheFlushEventId;
    }
}

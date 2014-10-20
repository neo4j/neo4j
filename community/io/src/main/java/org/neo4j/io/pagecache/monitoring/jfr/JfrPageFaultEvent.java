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

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.io.pagecache.monitoring.PageFaultEvent;

@EventDefinition(path = "neo4j/io/pagecache/fault")
public class JfrPageFaultEvent extends TimedEvent implements PageFaultEvent
{
    private final AtomicLong bytesReadTotal;

    @ValueDefinition(name = "pinEventId", relationKey = JfrPinEvent.REL_KEY_PIN_EVENT_ID)
    private long pinEventId;
    @ValueDefinition(name = "filePageId")
    private long filePageId;
    @ValueDefinition(name = "filename")
    private String filename;
    @ValueDefinition(name = "bytesRead", contentType = ContentType.Bytes)
    private int bytesRead;
    @ValueDefinition(name = "gotException")
    private boolean gotException;
    @ValueDefinition(name = "exceptionMessage")
    private String exceptionMessage;
    @ValueDefinition(name = "cachePageId")
    private int cachePageId;
    @ValueDefinition(name = "parked")
    private boolean parked;

    public JfrPageFaultEvent( AtomicLong bytesRead )
    {
        super( JfrPageCacheMonitor.faultToken );
        bytesReadTotal = bytesRead;
    }

    @Override
    public void addBytesRead( int bytes )
    {
        this.bytesRead += bytes;
        bytesReadTotal.getAndAdd( bytes );
    }

    @Override
    public void done()
    {
        end();
        commit();
    }

    @Override
    public void done( Throwable throwable )
    {
        this.gotException = true;
        this.exceptionMessage = throwable.getMessage();
        done();
    }

    public void setPinEventId( long pinEventId )
    {
        this.pinEventId = pinEventId;
    }

    public void setFilePageId( long filePageId )
    {
        this.filePageId = filePageId;
    }

    public void setFilename( String filename )
    {
        this.filename = filename;
    }

    public String getExceptionMessage()
    {
        return exceptionMessage;
    }

    public boolean getGotException()
    {
        return gotException;
    }

    public int getBytesRead()
    {
        return bytesRead;
    }

    public String getFilename()
    {
        return filename;
    }

    public long getFilePageId()
    {
        return filePageId;
    }

    public long getPinEventId()
    {
        return pinEventId;
    }

    public int getCachePageId()
    {
        return cachePageId;
    }

    @Override
    public void setCachePageId( int cachePageId )
    {
        this.cachePageId = cachePageId;
    }

    public boolean getParked()
    {
        return parked;
    }

    @Override
    public void setParked( boolean parked )
    {
        this.parked = parked;
    }
}

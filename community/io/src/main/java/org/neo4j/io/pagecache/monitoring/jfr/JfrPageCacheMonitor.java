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

import com.oracle.jrockit.jfr.EventToken;
import com.oracle.jrockit.jfr.InstantEvent;
import com.oracle.jrockit.jfr.Producer;

import java.io.File;
import java.net.URI;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.monitoring.CountingPageCacheMonitor;
import org.neo4j.io.pagecache.monitoring.EvictionRunEvent;
import org.neo4j.io.pagecache.monitoring.MajorFlushEvent;
import org.neo4j.io.pagecache.monitoring.PinEvent;

/**
 * A special CountingPageCacheMonitor that also produces Java Flight Recorder
 * events.
 */
public class JfrPageCacheMonitor extends CountingPageCacheMonitor
{
    static final String producerUri = "http://neo4j.com/io/pagecache/jfr";
    static final Producer producer;
    static final EventToken faultToken;
    static final EventToken evictionToken;
    static final EventToken flushToken;
    static final EventToken pinToken;
    static final EventToken mappedFileToken;
    static final EventToken unmappedFileToken;
    static final EventToken evictionRunToken;
    static final EventToken fileFlushToken;
    static final EventToken cacheFlushToken;

    static
    {
        producer = createProducer();
        faultToken = createToken( JfrPageFaultEvent.class );
        evictionToken = createToken( JfrEvictionEvent.class );
        flushToken = createToken( JfrFlushEvent.class );
        pinToken = createToken( JfrPinEvent.class );
        mappedFileToken = createToken( JfrMappedFileEvent.class );
        unmappedFileToken = createToken( JfrUnmappedFileEvent.class );
        evictionRunToken = createToken( JfrEvictionRunEvent.class );
        fileFlushToken = createToken( JfrFileFlushEvent.class );
        cacheFlushToken = createToken( JfrCacheFlushEvent.class );
        producer.register();
    }

    private static Producer createProducer()
    {
        try
        {
            return new Producer(
                    "PageCacheMonitor",
                    "Monitoring the runtime behaviour of the Neo4j PageCache",
                    new URI( producerUri ) );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            return null;
        }
    }

    private static EventToken createToken( Class<? extends InstantEvent> type )
    {
        try
        {
            return producer.addEvent( type );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            return null;
        }
    }

    private final AtomicLong evictionRunCounter = new AtomicLong();

    @Override
    public void mappedFile( File file )
    {
        JfrMappedFileEvent event = new JfrMappedFileEvent( file.getAbsolutePath() );
        event.commit();
        super.mappedFile( file );
    }

    @Override
    public void unmappedFile( File file )
    {
        JfrUnmappedFileEvent event = new JfrUnmappedFileEvent( file.getAbsolutePath() );
        event.commit();
        super.unmappedFile( file );
    }

    @Override
    public EvictionRunEvent beginPageEvictions( int expectedEvictions )
    {
        long evictionRunId = evictionRunCounter.incrementAndGet();

        JfrEvictionRunEvent event = new JfrEvictionRunEvent(
                evictions, evictionExceptions, flushes, bytesWritten );
        event.begin();
        event.setExpectedEvictions( expectedEvictions );
        event.setEvictionRun( evictionRunId );
        return event;
    }

    @Override
    public PinEvent beginPin( boolean exclusiveLock, long filePageId, PageSwapper swapper )
    {
        long pinEventId = pins.incrementAndGet();

        JfrPinEvent event = new JfrPinEvent( unpins, faults, bytesRead );
        event.setPinEventId( pinEventId );
        event.setExclusiveLock( exclusiveLock );
        event.setFilePageId( filePageId );
        event.setFilename( swapper.fileName() );
        event.begin();
        return event;
    }

    @Override
    public MajorFlushEvent beginFileFlush( PageSwapper swapper )
    {
        JfrFileFlushEvent event = new JfrFileFlushEvent( flushes, bytesWritten );
        event.begin();
        event.setFilename( swapper.fileName() );
        return event;
    }

    @Override
    public MajorFlushEvent beginCacheFlush()
    {
        JfrCacheFlushEvent event = new JfrCacheFlushEvent( flushes, bytesWritten );
        event.begin();
        return event;
    }
}

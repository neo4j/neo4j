/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.io.pagecache.tracing.cursor;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.SwitchPoint;
import java.util.Objects;

import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.tracing.EvictionEvent;
import org.neo4j.io.pagecache.tracing.FlushEvent;
import org.neo4j.io.pagecache.tracing.FlushEventOpportunity;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageFaultEvent;
import org.neo4j.io.pagecache.tracing.PinEvent;

import static org.neo4j.unsafe.impl.internal.dragons.FeatureToggles.packageFlag;

public class DefaultPageCursorTracer implements PageCursorTracer
{
    private long pins = 0L;
    private long unpins = 0L;
    private long faults = 0L;
    private long bytesRead = 0L;
    private long bytesWritten = 0L;
    private long evictions = 0L;
    private long flushes;

    private long cyclePinsStart;
    private long cycleUnpinsStart;
    private long cycleFaultsStart;
    private long cycleBytesReadStart;
    private long cycleBytesWrittenStart;
    private long cycleEvictionsStart;
    private long cycleFlushesStart;

    private PageCacheTracer pageCacheTracer;

    private static final MethodHandle beginPinMH;
    private static final SwitchPoint beginPinSwitchPoint;
    static
    {
        try
        {
            // A hidden setting to have pin/unpin monitoring enabled from the start by default.
            // NOTE: This flag is documented in jmx.asciidoc
            boolean alwaysEnabled = packageFlag( DefaultPageCursorTracer.class, "tracePinUnpin", true );

            MethodType type = MethodType.methodType( PinEvent.class );
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            MethodHandle monitoredPinMH = lookup.findVirtual( DefaultPageCursorTracer.class, "beginTracingPin", type );
            if ( alwaysEnabled )
            {
                beginPinMH = monitoredPinMH;
                beginPinSwitchPoint = null;
            }
            else
            {
                MethodHandle nullPinMH = lookup.findVirtual( DefaultPageCursorTracer.class, "beginNullPin", type );
                beginPinSwitchPoint = new SwitchPoint();
                beginPinMH = beginPinSwitchPoint.guardWithTest( nullPinMH, monitoredPinMH );
            }
        }
        catch ( Exception e )
        {
            throw new AssertionError( "Unexpected MethodHandle initiation error", e );
        }
    }

    /**
     * Enable monitoring of page pins and unpins, which is disabled by default for
     * performance reasons.
     *
     * This is a one-way operation; once monitoring of pinning and unpinning has been
     * enabled, it cannot be disabled again without restarting the JVM.
     */
    public static void enablePinUnpinTracing()
    {
        if ( beginPinSwitchPoint != null && !beginPinSwitchPoint.hasBeenInvalidated() )
        {
            SwitchPoint.invalidateAll( new SwitchPoint[]{ beginPinSwitchPoint } );
        }
    }

    public void init( PageCacheTracer pageCacheTracer )
    {
        this.pageCacheTracer = pageCacheTracer;
        this.cyclePinsStart = pins;
        this.cycleUnpinsStart = unpins;
        this.cycleFaultsStart = faults;
        this.cycleBytesReadStart = bytesRead;
        this.cycleBytesWrittenStart = bytesWritten;
        this.cycleEvictionsStart = evictions;
        this.cycleFlushesStart = flushes;
    }

    public void reportEvents()
    {
        Objects.nonNull( pageCacheTracer );
        pageCacheTracer.pins( Math.abs( pins - cyclePinsStart ) );
        pageCacheTracer.unpins( Math.abs( pins - cycleUnpinsStart ) );
        pageCacheTracer.faults( Math.abs( faults - cycleFaultsStart ) );
        pageCacheTracer.bytesRead( Math.abs( bytesRead - cycleBytesReadStart ) );
        pageCacheTracer.evictions( Math.abs( evictions - cycleEvictionsStart ) );
        pageCacheTracer.bytesWritten( Math.abs( bytesWritten - cycleBytesWrittenStart ) );
        pageCacheTracer.flushes( Math.abs( flushes - cycleFlushesStart ) );
    }

    @Override
    public long faults()
    {
        return faults;
    }

    @Override
    public long pins()
    {
        return pins;
    }

    @Override
    public long unpins()
    {
        return unpins;
    }

    @Override
    public long bytesRead()
    {
        return bytesRead;
    }

    @Override
    public PinEvent beginPin( boolean writeLock, long filePageId, PageSwapper swapper )
    {
        try
        {
            return (PinEvent) beginPinMH.invokeExact( this );
        }
        catch ( Throwable throwable )
        {
            throw new AssertionError( "Unexpected MethodHandle error", throwable );
        }
    }

    /**
     * Invoked through beginPinMH.
     */
    @SuppressWarnings( "UnusedDeclaration" )
    private PinEvent beginNullPin()
    {
        return nullPinEvent;
    }

    /**
     * Invoked through beginPinMH.
     */
    @SuppressWarnings( "UnusedDeclaration" )
    private PinEvent beginTracingPin()
    {
        pins++;
        return pinTracingEvent;
    }

    private final PinEvent pinTracingEvent = new PinEvent()
    {
        @Override
        public void setCachePageId( int cachePageId )
        {
        }

        @Override
        public PageFaultEvent beginPageFault()
        {
            return pageFaultEvent;
        }

        @Override
        public void done()
        {
            unpins++;
        }
    };

    private final PinEvent nullPinEvent = new PinEvent()
    {
        @Override
        public void setCachePageId( int cachePageId )
        {
        }

        @Override
        public PageFaultEvent beginPageFault()
        {
            return pageFaultEvent;
        }

        @Override
        public void done()
        {
        }
    };

    private final EvictionEvent evictionEvent = new EvictionEvent()
    {
        @Override
        public void setFilePageId( long filePageId )
        {
        }

        @Override
        public void setSwapper( PageSwapper swapper )
        {
        }

        @Override
        public FlushEventOpportunity flushEventOpportunity()
        {
            return flushEventOpportunity;
        }

        @Override
        public void threwException( IOException exception )
        {
        }

        @Override
        public void setCachePageId( int cachePageId )
        {
        }

        @Override
        public void close()
        {
            evictions++;
        }
    };

    private final PageFaultEvent pageFaultEvent = new PageFaultEvent()
    {
        @Override
        public void addBytesRead( long bytes )
        {
            bytesRead += bytes;
        }

        @Override
        public void done()
        {
            faults++;
        }

        @Override
        public void done( Throwable throwable )
        {
            done();
        }

        @Override
        public EvictionEvent beginEviction()
        {
            return evictionEvent;
        }

        @Override
        public void setCachePageId( int cachePageId )
        {
        }
    };

    private final FlushEventOpportunity flushEventOpportunity = new FlushEventOpportunity()
    {
        @Override
        public FlushEvent beginFlush( long filePageId, int cachePageId, PageSwapper swapper )
        {
            return flushEvent;
        }
    };

    private final FlushEvent flushEvent = new FlushEvent()
    {
        @Override
        public void addBytesWritten( long bytes )
        {
            bytesWritten += bytes;
        }

        @Override
        public void done()
        {
            flushes++;
        }

        @Override
        public void done( IOException exception )
        {
            done();
        }

        @Override
        public void addPagesFlushed( int pageCount )
        {
        }
    };

}

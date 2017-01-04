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
package org.neo4j.io.pagecache.tracing;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.SwitchPoint;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.io.pagecache.PageSwapper;

import static org.neo4j.unsafe.impl.internal.dragons.FeatureToggles.packageFlag;

/**
 * The default PageCacheTracer implementation, that just increments counters.
 */
public class DefaultPageCacheTracer implements PageCacheTracer
{
    protected final AtomicLong faults = new AtomicLong();
    protected final AtomicLong evictions = new AtomicLong();
    protected final AtomicLong pins = new AtomicLong();
    protected final AtomicLong unpins = new AtomicLong();
    protected final AtomicLong flushes = new AtomicLong();
    protected final AtomicLong bytesRead = new AtomicLong();
    protected final AtomicLong bytesWritten = new AtomicLong();
    protected final AtomicLong filesMapped = new AtomicLong();
    protected final AtomicLong filesUnmapped = new AtomicLong();
    protected final AtomicLong evictionExceptions = new AtomicLong();

    private final FlushEvent flushEvent = new FlushEvent()
    {
        @Override
        public void addBytesWritten( long bytes )
        {
            bytesWritten.getAndAdd( bytes );
        }

        @Override
        public void done()
        {
            flushes.getAndIncrement();
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

    private final FlushEventOpportunity flushEventOpportunity = new FlushEventOpportunity()
    {
        @Override
        public FlushEvent beginFlush( long filePageId, int cachePageId, PageSwapper swapper )
        {
            return flushEvent;
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
            evictionExceptions.getAndIncrement();
        }

        @Override
        public void setCachePageId( int cachePageId )
        {
        }

        @Override
        public void close()
        {
            evictions.getAndIncrement();
        }
    };

    private final EvictionRunEvent evictionRunEvent = new EvictionRunEvent()
    {
        @Override
        public EvictionEvent beginEviction()
        {
            return evictionEvent;
        }

        @Override
        public void close()
        {
        }
    };

    private final MajorFlushEvent majorFlushEvent = new MajorFlushEvent()
    {
        @Override
        public FlushEventOpportunity flushEventOpportunity()
        {
            return flushEventOpportunity;
        }

        @Override
        public void close()
        {
        }
    };

    @Override
    public void mappedFile( File file )
    {
        filesMapped.getAndIncrement();
    }

    @Override
    public void unmappedFile( File file )
    {
        filesUnmapped.getAndIncrement();
    }

    @Override
    public EvictionRunEvent beginPageEvictions( int pageCountToEvict )
    {
        return evictionRunEvent;
    }

    @Override
    public MajorFlushEvent beginFileFlush( PageSwapper swapper )
    {
        return majorFlushEvent;
    }

    @Override
    public MajorFlushEvent beginCacheFlush()
    {
        return majorFlushEvent;
    }

    @Override
    public long faults()
    {
        return faults.get();
    }

    @Override
    public long evictions()
    {
        return evictions.get();
    }

    @Override
    public long pins()
    {
        return pins.get();
    }

    @Override
    public long unpins()
    {
        return unpins.get();
    }

    @Override
    public long flushes()
    {
        return flushes.get();
    }

    @Override
    public long bytesRead()
    {
        return bytesRead.get();
    }

    @Override
    public long bytesWritten()
    {
        return bytesWritten.get();
    }

    @Override
    public long filesMapped()
    {
        return filesMapped.get();
    }

    @Override
    public long filesUnmapped()
    {
        return filesUnmapped.get();
    }

    @Override
    public long evictionExceptions()
    {
        return evictionExceptions.get();
    }

    @Override
    public void pins( long pins )
    {
        this.pins.getAndAdd( pins );
    }

    @Override
    public void unpins( long unpins )
    {
        this.unpins.getAndAdd( unpins );
    }

    @Override
    public void faults( long faults )
    {
        this.faults.getAndAdd( faults );
    }

    @Override
    public void bytesRead( long bytesRead )
    {
        this.bytesRead.getAndAdd( bytesRead );
    }
}

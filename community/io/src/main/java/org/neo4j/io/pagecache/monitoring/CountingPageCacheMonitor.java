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
package org.neo4j.io.pagecache.monitoring;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.io.pagecache.PageSwapper;

public class CountingPageCacheMonitor implements PageCacheMonitor
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
        public void addBytesWritten( int bytes )
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

    private final PageFaultEvent pageFaultEvent = new PageFaultEvent()
    {
        @Override
        public void addBytesRead( int bytes )
        {
            bytesRead.getAndAdd( bytes );
        }

        @Override
        public void done()
        {
            faults.getAndIncrement();
        }

        @Override
        public void done( Throwable throwable )
        {
            done();
        }

        @Override
        public void setCachePageId( int cachePageId )
        {
        }

        @Override
        public void setParked( boolean parked )
        {
        }
    };

    private final PinEvent pinEvent = new PinEvent()
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
            unpins.getAndIncrement();
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
    public PinEvent beginPin( boolean exclusiveLock, long filePageId, PageSwapper swapper )
    {
        pins.getAndIncrement();
        return pinEvent;
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

    public long countFaults()
    {
        return faults.get();
    }

    public long countEvictions()
    {
        return evictions.get();
    }

    public long countPins()
    {
        return pins.get();
    }

    public long countUnpins()
    {
        return unpins.get();
    }

    public long countFlushes()
    {
        return flushes.get();
    }

    public long countBytesRead()
    {
        return bytesRead.get();
    }

    public long countBytesWritten()
    {
        return bytesWritten.get();
    }

    public long countFilesMapped()
    {
        return filesMapped.get();
    }

    public long countFilesUnmapped()
    {
        return filesUnmapped.get();
    }

    public long countEvictionExceptions()
    {
        return evictionExceptions.get();
    }
}

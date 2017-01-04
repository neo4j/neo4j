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
package org.neo4j.io.pagecache;

import org.neo4j.io.pagecache.tracing.EvictionEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageFaultEvent;
import org.neo4j.io.pagecache.tracing.PinEvent;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

//TODO:
public class RecordingPageCursorTracer implements PageCursorTracer
{
    @Override
    public long faults()
    {
        return 0;
    }

    @Override
    public long pins()
    {
        return 0;
    }

    @Override
    public long unpins()
    {
        return 0;
    }

    @Override
    public long bytesRead()
    {
        return 0;
    }

    @Override
    public PinEvent beginPin( boolean writeLock, final long filePageId, final PageSwapper swapper )
    {
        return new PinEvent()
        {
            @Override
            public void setCachePageId( int cachePageId )
            {
            }

            @Override
            public PageFaultEvent beginPageFault()
            {
                return new PageFaultEvent()
                {
                    @Override
                    public void addBytesRead( long bytes )
                    {
                    }

                    @Override
                    public void done()
                    {
                        pageFaulted( filePageId, swapper );
                    }

                    @Override
                    public void done( Throwable throwable )
                    {
                    }

                    @Override
                    public EvictionEvent beginEviction()
                    {
                        return EvictionEvent.NULL;
                    }

                    @Override
                    public void setCachePageId( int cachePageId )
                    {
                    }
                };
            }

            @Override
            public void done()
            {
                pinned( filePageId, swapper );
            }
        };
    }

    @Override
    public void init( PageCacheTracer tracer )
    {

    }

    @Override
    public void reportEvents()
    {

    }

    private void pageFaulted( long filePageId, PageSwapper swapper )
    {
//        record( new Fault( swapper, filePageId ) );
    }

    private void pinned( long filePageId, PageSwapper swapper )
    {
//        record( new Pin( swapper, filePageId ) );
    }
}

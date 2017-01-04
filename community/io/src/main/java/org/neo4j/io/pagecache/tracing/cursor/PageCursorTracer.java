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

import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.PinEvent;

//:TODO javadoc
public interface PageCursorTracer
{

    PageCursorTracer NULL = new PageCursorTracer()
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
        public PinEvent beginPin( boolean writeLock, long filePageId, PageSwapper swapper )
        {
            return PinEvent.NULL;
        }

        @Override
        public void init( PageCacheTracer tracer )
        {

        }

        @Override
        public void reportEvents()
        {

        }
    };

    /**
     * @return The number of page faults observed thus far.
     */
    long faults();

    /**
     * @return The number of page pins observed thus far.
     */
    long pins();

    /**
     * @return The number of page unpins observed thus far.
     */
    long unpins();

    /**
     * @return The sum total of bytes read in through page faults thus far.
     */
    long bytesRead();

    PinEvent beginPin( boolean writeLock, long filePageId, PageSwapper swapper );

    // TODO:: methods that glue current tracer to page cache tracer can be avoided?
    void init( PageCacheTracer tracer );

    void reportEvents();
}

/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

/**
 * Begin pinning a page.
 */
public interface PinEvent
{
    /**
     * A PinEvent that does nothing other than return the PageFaultEvent.NULL.
     */
    PinEvent NULL = new PinEvent()
    {
        @Override
        public void setCachePageId( int cachePageId )
        {
        }

        @Override
        public PageFaultEvent beginPageFault()
        {
            return PageFaultEvent.NULL;
        }

        @Override
        public void done()
        {
        }
    };

    /**
     * The id of the cache page that holds the file page we pinned.
     */
    public void setCachePageId( int cachePageId );

    /**
     * The page we want to pin is not in memory, so being a page fault to load it in.
     */
    public PageFaultEvent beginPageFault();

    /**
     * The pinning has completed and the page is now unpinned.
     */
    public void done();
}

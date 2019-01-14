/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
        public void setCachePageId( long cachePageId )
        {
        }

        @Override
        public PageFaultEvent beginPageFault()
        {
            return PageFaultEvent.NULL;
        }

        @Override
        public void hit()
        {
        }

        @Override
        public void done()
        {
        }
    };

    /**
     * The id of the cache page that holds the file page we pinned.
     */
    void setCachePageId( long cachePageId );

    /**
     * The page we want to pin is not in memory, so being a page fault to load it in.
     */
    PageFaultEvent beginPageFault();

    /**
     * Page found and bounded.
     */
    void hit();

    /**
     * The pinning has completed and the page is now unpinned.
     */
    void done();
}

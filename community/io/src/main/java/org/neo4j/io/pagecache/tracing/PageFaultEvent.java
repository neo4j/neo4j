/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
 * Begin a page fault as part of a pin event.
 */
public interface PageFaultEvent
{
    /**
     * A PageFaultEvent that does nothing.
     */
    PageFaultEvent NULL = new PageFaultEvent()
    {
        @Override
        public void addBytesRead( int bytes )
        {
        }

        @Override
        public void done()
        {
        }

        @Override
        public void done( Throwable throwable )
        {
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

    /**
     * Add up a number of bytes that has been read from the backing file into the free page being bound.
     */
    public void addBytesRead( int bytes );

    /**
     * The id of the cache page that is being faulted into.
     */
    public void setCachePageId( int cachePageId );

    /**
     * Set to 'true' if the page faulting thread ended up parking, while it waited for the eviction thread
     * to free up a page that could be faulted into.
     */
    public void setParked( boolean parked );

    /**
     * The page fault completed successfully.
     */
    public void done();

    /**
     * The page fault did not complete successfully, but instead caused the given Throwable to be thrown.
     */
    public void done( Throwable throwable );
}

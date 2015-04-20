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

import java.io.IOException;

/**
 * Begin flushing modifications from an in-memory page to the backing file.
 */
public interface FlushEvent
{
    /**
     * A FlushEvent implementation that does nothing.
     */
    FlushEvent NULL = new FlushEvent()
    {
        @Override
        public void addBytesWritten( int bytes )
        {
        }

        @Override
        public void done()
        {
        }

        @Override
        public void done( IOException exception )
        {
        }
    };

    /**
     * Add up a number of bytes that has been written to the file.
     */
    public void addBytesWritten( int bytes );

    /**
     * The page flush has completed successfully.
     */
    public void done();

    /**
     * The page flush did not complete successfully, but threw the given exception.
     */
    public void done( IOException exception );
}

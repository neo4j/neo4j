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
package org.neo4j.index.internal.gbptree;

import java.util.function.Consumer;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;

/**
 * Defines interfaces and common implementations of header reader/writer for {@link GBPTree}.
 */
public class Header
{
    /**
     * Writes a header into a {@link GBPTree} state page during
     * {@link GBPTree#checkpoint(org.neo4j.io.pagecache.IOLimiter)}.
     */
    public interface Writer
    {
        /**
         * Writes header data into {@code to} with previous valid header data found in {@code from} of {@code length}
         * bytes in size.
         *
         * @param from {@link PageCursor} positioned at the header data written in the previous check point.
         * @param length size in bytes of the previous header data.
         * @param to {@link PageCursor} to write new header into.
         */
        void write( PageCursor from, int length, PageCursor to );
    }

    static final Writer CARRY_OVER_PREVIOUS_HEADER = ( from, length, to ) ->
    {
        int toOffset = to.getOffset();
        from.copyTo( from.getOffset(), to, toOffset, length );
        to.setOffset( toOffset + length );
    };

    static Writer replace( Consumer<PageCursor> writer )
    {
        // Discard the previous state, just write the new
        return ( from, length, to ) -> writer.accept( to );
    }

    /**
     * Reads a header from a {@link GBPTree} state page during opening it.
     */
    public interface Reader
    {
        /**
         * Called when it's time to read header data from the most up to date and valid state page.
         * Due to the nature of the underlying {@link PageCache} this method may be called several times,
         * some times with invalid data in the {@link PageCursor}. Because of this there mustn't be any
         * exceptions thrown or decisions made based on the read data until the GBPTree constructor has been
         * completely executed.
         *
         * @param from {@link PageCursor} positioned at beginning of the header data to read.
         * @param length number of bytes available to read in the header data.
         */
        void read( PageCursor from, int length );
    }
}

/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.kernel.api;

/**
 * Initializer for spreading a scan operator over multiple cursors for use from different threads in parallel.
 *
 * @param <Cursor>
 *         the type of cursor this object initializes.
 */
public interface Scan<Cursor extends org.neo4j.internal.kernel.api.Cursor>
{
    /**
     * Will attempt to reserve a batch to scan.
     * <p>
     * A <code>Scan</code> instance can be shared among threads and guarantees that each call to
     * <code>reserveBatch</code> will
     * reserve exclusive ranges for the scan. The basic usage pattern is that a single <code>Scan</code> scan instance
     * is shared among several threads but where each thread maintains separate cursors. Each thread can call
     * <code>reserveBatch</code> multiple times and then proceed to iterate the cursor as usual.
     * <p>
     * Example:
     * <pre>
     * {@code
     *   try ( NodeCursor cursor = cursors.allocateCursor() )
     *   {
     *     while ( scan.reserveBatch( cursor, 42 ) )
     *     {
     *       while ( cursor.next() )
     *       {
     *         //do things with the node
     *       }
     *     }
     *   }
     * }
     * </pre>
     * <p>
     * Using it this way will guarantee that each scan - regardless of the calling thread - will see different parts of
     * the underlying data.
     *
     * @param cursor The cursor to be used for reading.
     * @param sizeHint The approximate size the batch, the provided size must be greater than 0.
     * @return <code>true</code> if there are more data to read, otherwise <code>false</code>
     */
    boolean reserveBatch( Cursor cursor, int sizeHint );
}

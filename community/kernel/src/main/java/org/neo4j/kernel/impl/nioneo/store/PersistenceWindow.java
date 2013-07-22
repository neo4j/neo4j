/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

/**
 * A persistence window encapsulates a part of the records (or blocks) in a
 * store and makes it possible to read and write data to those records.
 */
public interface PersistenceWindow
{
    /**
     * Returns the underlying buffer to this persistence window. Since a window
     * may hold many records this gives access to all these records in one buffer.
     * Changes to the returned buffer are applied when calling {@link #force()}.
     * 
     * @return the underlying buffer.
     */
    Buffer getBuffer();
    
    /**
     * Returns the underlying buffer set at a specific record ({@code id}}.
     * The id is the absolute record/block id of the whole underlying channel, which this
     * window just provides its limited view of.
     * Changes to the returned buffer are applied when calling {@link #force()}.
     * 
     * @param id the record/block to offset the buffer to before returning it.
     * @return the underlying buffer.
     */
    Buffer getOffsettedBuffer( long id );

    /**
     * @return the record size for each record. A window can hold many records.
     */
    int getRecordSize();
    
    /**
     * @return the current absolute record/block position of the first record
     * in this window.
     */
    long position();

    /**
     * @return the size of this window meaning the number of records/blocks it
     * encapsulates.
     */
    int size();

    /**
     * Force (write) changes to the underlying buffer returned from {@link #getBuffer()}
     * and {@link #getOffsettedBuffer(long)}.
     */
    void force();

    /**
     * Just closes the window without writing any potential changes made to it.
     */
    void close();
}
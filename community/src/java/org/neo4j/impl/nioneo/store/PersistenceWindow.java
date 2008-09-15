/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.nioneo.store;

/**
 * A persistence window encapsulates a part of the records (or blocks) in a
 * store and makes it possible to read and write data to those records.
 */
public interface PersistenceWindow
{
    /**
     * Returns the underlying buffer to this persistence window.
     * 
     * @return The underlying buffer
     */
    public Buffer getBuffer();

    /**
     * Returns the current record/block position.
     * 
     * @return The current position
     */
    public long position();

    /**
     * Returns the size of this window meaning the number of records/blocks it
     * encapsulates.
     * 
     * @return The window size
     */
    public int size();

    public void force();

    public void close();
}
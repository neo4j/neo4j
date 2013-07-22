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
package org.neo4j.kernel.api.index;

import java.io.IOException;

/**
 * Used for online operation of an index.
 */
public interface IndexAccessor
{
    /**
     * Deletes this index as well as closes all used external resources.
     * There will not be any interactions after this call.
     * 
     * @throws IOException if unable to drop index.
     */
    void drop() throws IOException;

    /**
     * Apply a set of changes to this index.
     * Updates must be visible in {@link #newReader() readers} created after this update.
     */
    void updateAndCommit( Iterable<NodePropertyUpdate> updates ) throws IOException, IndexEntryConflictException;

    /**
     * Apply a set of changes to this index. This method will be called instead of
     * {@link #updateAndCommit(Iterable)} during recovery of the database when starting up after
     * a crash or similar. Updates given here may have already been applied to this index, so
     * additional checks must be in place so that data doesn't get duplicated, but is idempotent.
     */
    void recover( Iterable<NodePropertyUpdate> updates ) throws IOException;
    
    /**
     * Forces this index to disk. Called at certain points from within Neo4j for example when
     * rotating the logical log. After completion of this call there cannot be any essential state that
     * hasn't been forced to disk.
     * 
     * @throws IOException if there was a problem forcing the state to persistent storage.
     */
    void force() throws IOException;
    
    /**
     * Closes this index accessor. There will not be any interactions after this call.
     * After completion of this call there cannot be any essential state that hasn't been forced to disk.
     * 
     * @throws IOException if unable to close index.
     */
    void close() throws IOException;
    
    /**
     * @return a new {@link IndexReader} responsible for looking up results in the index. The returned
     * reader must honor repeatable reads.
     */
    IndexReader newReader();

    class Adapter implements IndexAccessor
    {
        @Override
        public void drop()
        {
        }

        @Override
        public void updateAndCommit( Iterable<NodePropertyUpdate> updates ) throws IndexEntryConflictException,
                IOException
        {
        }
        
        @Override
        public void recover( Iterable<NodePropertyUpdate> updates ) throws IOException
        {
        }

        @Override
        public void force()
        {
        }

        @Override
        public void close()
        {
        }

        @Override
        public IndexReader newReader()
        {
            return new IndexReader.Empty();
        }
    }
}

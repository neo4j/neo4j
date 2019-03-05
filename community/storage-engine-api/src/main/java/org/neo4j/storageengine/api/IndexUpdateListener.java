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
package org.neo4j.storageengine.api;

import java.io.IOException;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.schema.SchemaDescriptor;

public interface IndexUpdateListener
{
    /**
     * One or more indexes were created. This listener should take care of managing initial population of it.
     * @param indexes indexes that were created.
     */
    void createIndexes( StorageIndexReference... indexes );

    /**
     * Used when activating an index after it has been created and populated.
     * Effectively this is used when activating a constraint index, where the constraint after population goes though
     * some validation and finally gets activated.
     *
     * @param index index to activate.
     * @throws KernelException if index failed to be activated.
     */
    void activateIndex( StorageIndexReference index ) throws KernelException;

    /**
     * Drops an index.
     * @param index index to be dropped.
     */
    void dropIndex( StorageIndexReference index );

    /**
     * Applies indexing updates from changes in underlying storage.
     * @param updates stream of updates to apply.
     */
    void applyUpdates( Iterable<IndexEntryUpdate<SchemaDescriptor>> updates ) throws IOException, KernelException;

    /**
     * Called before commit to ask whether or not the particular indexReference is valid.
     * @param indexReference reference to the index being validated.
     * @throws KernelException if index isn't valid.
     */
    void validateIndex( long indexReference ) throws KernelException;

    class Adapter implements IndexUpdateListener
    {
        @Override
        public void createIndexes( StorageIndexReference[] indexes )
        {
        }

        @Override
        public void activateIndex( StorageIndexReference index ) throws KernelException
        {
        }

        @Override
        public void dropIndex( StorageIndexReference index )
        {
        }

        @Override
        public void applyUpdates( Iterable<IndexEntryUpdate<SchemaDescriptor>> updates ) throws IOException, KernelException
        {
        }

        @Override
        public void validateIndex( long indexReference ) throws KernelException
        {
        }
    }
}

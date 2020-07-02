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
package org.neo4j.storageengine.api;

import java.io.IOException;

import org.neo4j.common.Subject;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

public interface IndexUpdateListener
{
    /**
     * One or more indexes were created. This listener should take care of managing initial population of it.
     * @param subject subject that triggered the index creation.
     * This is used for monitoring purposes, so work related to index creation and population can be linked to its originator.
     * @param indexes indexes that were created.
     */
    void createIndexes( Subject subject, IndexDescriptor... indexes );

    /**
     * Used when activating an index after it has been created and populated.
     * Effectively this is used when activating a constraint index, where the constraint after population goes though
     * some validation and finally gets activated.
     *
     * @param index index to activate.
     * @throws KernelException if index failed to be activated.
     */
    void activateIndex( IndexDescriptor index ) throws KernelException;

    /**
     * Drops an index.
     * @param index index to be dropped.
     */
    void dropIndex( IndexDescriptor index );

    /**
     * Applies indexing updates from changes in underlying storage.
     * @param updates stream of updates to apply.
     * @param cursorTracer underlying page cursor tracer
     */
    void applyUpdates( Iterable<IndexEntryUpdate<IndexDescriptor>> updates, PageCursorTracer cursorTracer ) throws IOException, KernelException;

    /**
     * Called before commit to ask whether or not the particular indexReference is valid.
     * @param indexReference reference to the index being validated.
     * @throws KernelException if index isn't valid.
     */
    void validateIndex( long indexReference ) throws KernelException;

    class Adapter implements IndexUpdateListener
    {
        @Override
        public void createIndexes( Subject subject, IndexDescriptor... indexes )
        {
        }

        @Override
        public void activateIndex( IndexDescriptor index )
        {
        }

        @Override
        public void dropIndex( IndexDescriptor index )
        {
        }

        @Override
        public void applyUpdates( Iterable<IndexEntryUpdate<IndexDescriptor>> updates, PageCursorTracer cursorTracer )
        {
        }

        @Override
        public void validateIndex( long indexReference )
        {
        }
    }
}

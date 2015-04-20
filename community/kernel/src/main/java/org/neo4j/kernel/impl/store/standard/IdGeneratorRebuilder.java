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
package org.neo4j.kernel.impl.store.standard;

import java.io.IOException;

import org.neo4j.kernel.impl.store.format.Store;

import static org.neo4j.kernel.impl.store.format.Store.SF_REVERSE_CURSOR;
import static org.neo4j.kernel.impl.store.format.Store.SF_SCAN;

/**
 * Given that a store has not shut down properly, this component is responsible for inspecting the store and rebuilding
 * the stores associated id generator. Specifically, rebuilding means ensuring the id generator will never allocate an
 * id that is in use in the store and, optionally, ensuring records that are not in use are noted as available in the
 * id generator.
 *
 * So, for illustration, a valid rebuilder could look at the file size and calculate the max possible id in use, and
 * simply start allocating ids past that. This guarantees we'd never allocate an id of a record that is in use,
 * although it would cause a lot of fragmentation.
 *
 * These rebuilders are ran with the guarantee that no other reading or writing will be concurrently going on against
 * the store or id generator.
 */
public interface IdGeneratorRebuilder
{
    interface Factory
    {
        IdGeneratorRebuilder newIdGeneratorRebuilder( Store<?,?> store, StoreToolkit toolkit, StoreIdGenerator idGenerator );
    }

    /**
     * Trigger this rebuilder to rebuild the id generator it wraps.
     */
    void rebuildIdGenerator() throws IOException;

    /**
     * A fast strategy that scans the store from the end, backwards, until it finds a record in use, and just marks
     * that as the highest one in use. This gives very fast startup times at the expense of lost disk space.
     */
    public static class FindHighestInUseRebuilderFactory implements IdGeneratorRebuilder.Factory
    {
        @Override
        public IdGeneratorRebuilder newIdGeneratorRebuilder( final Store<?,?> store, final StoreToolkit toolkit, final StoreIdGenerator idGenerator )
        {
            return new IdGeneratorRebuilder()
            {
                @Override
                public void rebuildIdGenerator() throws IOException
                {
                    try(Store.RecordCursor<?> cursor = store.cursor( SF_REVERSE_CURSOR | SF_SCAN ))
                    {
                        cursor.position( (toolkit.fileSize() / toolkit.recordSize()) + 1 );
                        cursor.next(); // Moves to the next (backwards!) record in use
                        idGenerator.rebuild( Math.max(cursor.recordId(), toolkit.firstRecordId()) );
                    }
                }
            };
        }
    }

    /**
     * A strategy that scans the whole store, finding every single unused record. This leaves the id generator ready
     * to fully defragment the store at the expense of startup time after a crash.
     */
    public static class FullDefragmentationRebuilderFactory implements IdGeneratorRebuilder.Factory
    {
        @Override
        public IdGeneratorRebuilder newIdGeneratorRebuilder( final Store<?,?> store, final StoreToolkit toolkit, final StoreIdGenerator idGenerator )
        {
            return new IdGeneratorRebuilder()
            {
                @Override
                public void rebuildIdGenerator() throws IOException
                {
                    // First, find the highest in use, scanning from the back:
                    new FindHighestInUseRebuilderFactory()
                            .newIdGeneratorRebuilder( store, toolkit, idGenerator ).rebuildIdGenerator();

                    long highId = idGenerator.highestIdInUse();

                    // Then, scan the full store from the beginning and find each unused record
                    try(Store.RecordCursor<?> cursor = store.cursor( SF_SCAN ))
                    {
                        long currentId = toolkit.firstRecordId();

                        // Scan store
                        while(currentId < highId && cursor.position( currentId++ ))
                        {
                            boolean inUse;
                            do
                            {
                                inUse = cursor.inUse();
                            }
                            while ( cursor.shouldRetry() );
                            if( !inUse )
                            {
                                idGenerator.free( cursor.recordId() );
                            }
                        }
                    }
                }
            };
        }
    }
}

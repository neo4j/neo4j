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
package org.neo4j.kernel.impl.transaction.command;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.kernel.api.exceptions.index.IndexActivationFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.impl.api.index.IndexingService;

/**
 * Delayed activation of indexes. At the point in time when a transaction that creates a uniqueness constraint
 * commits it may be that some low-level locks are held on nodes/relationships, locks which prevents the backing index from being
 * fully populated. Those locks are released after the appliers have been closed. This activator acts as a register for indexes
 * that wants to be activated inside an applier, to be activated right after the low-level locks have been released for the batch
 * of transactions currently applying.
 */
public class IndexActivator implements AutoCloseable
{
    private final IndexingService indexingService;
    private Set<Long> indexesToActivate;

    public IndexActivator( IndexingService indexingService )
    {
        this.indexingService = indexingService;
    }

    /**
     * Activates any index that needs activation, i.e. have been added with {@link #activateIndex(long)}.
     */
    @Override
    public void close()
    {
        if ( indexesToActivate != null )
        {
            for ( long indexId : indexesToActivate )
            {
                try
                {
                    indexingService.activateIndex( indexId );
                }
                catch ( IndexNotFoundKernelException | IndexActivationFailedKernelException | IndexPopulationFailedKernelException e )
                {
                    throw new IllegalStateException( "Unable to enable constraint, backing index is not online.", e );
                }
            }
        }
    }

    /**
     * Makes a note to activate index after batch of transaction have been applied, i.e. in {@link #close()}.
     * @param indexId index id.
     */
    void activateIndex( long indexId )
    {
        if ( indexesToActivate == null )
        {
            indexesToActivate = new HashSet<>();
        }
        indexesToActivate.add( indexId );
    }

    /**
     * Called when an index is dropped, so that a previously noted index to activate is removed from this internal list.
     * @param indexId index id.
     */
    void indexDropped( long indexId )
    {
        if ( indexesToActivate != null )
        {
            indexesToActivate.remove( indexId );
        }
    }
}

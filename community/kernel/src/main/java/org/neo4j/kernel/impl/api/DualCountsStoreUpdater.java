/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import java.util.Optional;

import org.neo4j.kernel.impl.store.counts.CountsStorageService;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.storageengine.api.TransactionApplicationMode;

public class DualCountsStoreUpdater implements CountsAccessor.Updater
{
    private final CountsTracker.Updater countsTrackerUpdater;
    private final CountsAccessor.Updater countsStorageUpdater;

    public DualCountsStoreUpdater( long txId, CountsStorageService countsStorageService, CountsTracker countsTracker,
            TransactionApplicationMode mode )
    {
        countsStorageUpdater = countsStorageService.apply( txId, mode );
        Optional<CountsAccessor.Updater> result = countsTracker.apply( txId );
        this.countsTrackerUpdater = result.orElse( null );
        assert this.countsTrackerUpdater != null || mode == TransactionApplicationMode.RECOVERY;
    }

    @Override
    public void incrementNodeCount( int labelId, long delta )
    {
        if ( countsTrackerUpdater != null )
        {
            countsTrackerUpdater.incrementNodeCount( labelId, delta );
        }
        countsStorageUpdater.incrementNodeCount( labelId, delta );
    }

    @Override
    public void incrementRelationshipCount( int startLabelId, int typeId, int endLabelId, long delta )
    {
        if ( countsTrackerUpdater != null )
        {
            countsTrackerUpdater.incrementRelationshipCount( startLabelId, typeId, endLabelId, delta );
        }
        countsStorageUpdater.incrementRelationshipCount( startLabelId, typeId, endLabelId, delta );
    }

    @Override
    public void close()
    {
        if ( countsTrackerUpdater != null )
        {
            countsTrackerUpdater.close();
        }
        countsStorageUpdater.close();
    }
}
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
package org.neo4j.kernel.impl.api.index;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.Reservation;

/**
 * Index updates that are validated and intended to be {@link #flush(Consumer) flushed} into an
 * {@link OnlineIndexProxy online index}.
 */
class OnlineValidatedIndexUpdates implements ValidatedIndexUpdates
{
    private final Reservation reservation;
    private final Map<IndexDescriptor,List<NodePropertyUpdate>> updatesByIndex;
    private final IndexUpdaterMap indexUpdaters;

    OnlineValidatedIndexUpdates( Reservation reservation,
            Map<IndexDescriptor,List<NodePropertyUpdate>> updatesByIndex, IndexUpdaterMap indexUpdaters )
    {
        this.reservation = reservation;
        this.updatesByIndex = updatesByIndex;
        this.indexUpdaters = indexUpdaters;
    }

    @Override
    public void flush( Consumer<IndexDescriptor> affectedIndexes )
            throws IOException, IndexEntryConflictException, IndexCapacityExceededException
    {
        for ( Map.Entry<IndexDescriptor,List<NodePropertyUpdate>> entry : updatesByIndex.entrySet() )
        {
            IndexDescriptor indexDescriptor = entry.getKey();
            affectedIndexes.accept( indexDescriptor );
            List<NodePropertyUpdate> updates = entry.getValue();

            IndexUpdater updater = indexUpdaters.getUpdater( indexDescriptor );
            for ( NodePropertyUpdate update : updates )
            {
                updater.process( update );
            }
        }
    }

    @Override
    public void close()
    {
        try
        {
            reservation.release();
        }
        finally
        {
            indexUpdaters.close();
        }
    }

    @Override
    public boolean hasChanges()
    {
        return !updatesByIndex.isEmpty();
    }
}

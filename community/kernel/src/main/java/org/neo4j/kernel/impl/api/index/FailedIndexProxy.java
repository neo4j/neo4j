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
package org.neo4j.kernel.impl.api.index;

import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.IndexDropper;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.values.storable.Value;

import static org.neo4j.internal.helpers.collection.Iterators.emptyResourceIterator;

public class FailedIndexProxy extends AbstractSwallowingIndexProxy
{
    private final IndexDropper indexDropper;
    private final String indexUserDescription;
    private final IndexStatisticsStore indexStatisticsStore;
    private final Log log;

    FailedIndexProxy( IndexDescriptor descriptor,
            String indexUserDescription,
            IndexDropper indexDropper,
            IndexPopulationFailure populationFailure,
            IndexStatisticsStore indexStatisticsStore,
            LogProvider logProvider )
    {
        super( descriptor, populationFailure );
        this.indexDropper = indexDropper;
        this.indexUserDescription = indexUserDescription;
        this.indexStatisticsStore = indexStatisticsStore;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void drop()
    {
        indexStatisticsStore.removeIndex( getDescriptor().getId() );
        String message = "FailedIndexProxy#drop index on " + indexUserDescription + " dropped due to:\n" +
                     getPopulationFailure().asString();
        log.info( message );
        indexDropper.drop();
    }

    @Override
    public InternalIndexState getState()
    {
        return InternalIndexState.FAILED;
    }

    @Override
    public boolean awaitStoreScanCompleted( long time, TimeUnit unit ) throws IndexPopulationFailedKernelException
    {
        throw failureCause();
    }

    private IndexPopulationFailedKernelException failureCause()
    {
        return getPopulationFailure().asIndexPopulationFailure( getDescriptor().schema(), indexUserDescription );
    }

    @Override
    public void activate()
    {
        throw new UnsupportedOperationException( "Cannot activate a failed index." );
    }

    @Override
    public void validate() throws IndexPopulationFailedKernelException
    {
        throw failureCause();
    }

    @Override
    public void validateBeforeCommit( Value[] tuple )
    {
    }

    @Override
    public ResourceIterator<File> snapshotFiles()
    {
        return emptyResourceIterator();
    }

    @Override
    public Map<String,Value> indexConfig()
    {
        return indexDropper.indexConfig();
    }
}

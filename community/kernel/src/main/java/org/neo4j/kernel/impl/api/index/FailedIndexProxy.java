/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Future;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.helpers.FutureAdapter.VOID;
import static org.neo4j.helpers.collection.IteratorUtil.emptyIterator;

public class FailedIndexProxy extends AbstractSwallowingIndexProxy
{
    protected final IndexPopulator populator;
    private final String indexUserDescription;
    private final IndexCountsRemover indexCountsRemover;
    private final Log log;

    public FailedIndexProxy(IndexDescriptor descriptor,
                            IndexConfiguration configuration,
                            SchemaIndexProvider.Descriptor providerDescriptor,
                            String indexUserDescription,
                            IndexPopulator populator,
                            IndexPopulationFailure populationFailure,
                            IndexCountsRemover indexCountsRemover,
                            LogProvider logProvider )
    {
        super( descriptor, providerDescriptor, populationFailure, configuration );
        this.populator = populator;
        this.indexUserDescription = indexUserDescription;
        this.indexCountsRemover = indexCountsRemover;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public Future<Void> drop() throws IOException
    {
        indexCountsRemover.remove();
        String message = "FailedIndexProxy#drop index on " + indexUserDescription + " dropped due to:\n" +
                     getPopulationFailure().asString();
        log.info( message );
        populator.drop();
        return VOID;
    }

    @Override
    public InternalIndexState getState()
    {
        return InternalIndexState.FAILED;
    }

    @Override
    public boolean awaitStoreScanCompleted() throws IndexPopulationFailedKernelException
    {
        throw getPopulationFailure().asIndexPopulationFailure( getDescriptor(), indexUserDescription );
    }

    @Override
    public void activate()
    {
        throw new UnsupportedOperationException( "Cannot activate a failed index." );
    }

    @Override
    public void validate() throws IndexPopulationFailedKernelException
    {
        throw getPopulationFailure().asIndexPopulationFailure( getDescriptor(), indexUserDescription );
    }

    @Override
    public ResourceIterator<File> snapshotFiles()
    {
        return emptyIterator();
    }
}

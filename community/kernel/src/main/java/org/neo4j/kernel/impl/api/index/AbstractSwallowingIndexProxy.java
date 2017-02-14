/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.util.concurrent.Future;

import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.schema.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.PopulationProgress;
import org.neo4j.kernel.impl.api.index.updater.SwallowingIndexUpdater;

import static org.neo4j.helpers.FutureAdapter.VOID;

public abstract class AbstractSwallowingIndexProxy implements IndexProxy
{
    private final NewIndexDescriptor descriptor;
    private final SchemaIndexProvider.Descriptor providerDescriptor;
    private final IndexPopulationFailure populationFailure;

    public AbstractSwallowingIndexProxy( NewIndexDescriptor descriptor,
            SchemaIndexProvider.Descriptor providerDescriptor, IndexPopulationFailure populationFailure )
    {
        this.descriptor = descriptor;
        this.providerDescriptor = providerDescriptor;
        this.populationFailure = populationFailure;
    }

    @Override
    public IndexPopulationFailure getPopulationFailure()
    {
        return populationFailure;
    }

    @Override
    public PopulationProgress getIndexPopulationProgress()
    {
        return PopulationProgress.NONE;
    }

    @Override
    public void start()
    {
        String message = "Unable to start index, it is in a " + getState().name() + " state.";
        throw new UnsupportedOperationException( message + ", caused by: " + getPopulationFailure() );
    }

    @Override
    public IndexUpdater newUpdater( IndexUpdateMode mode )
    {
        return SwallowingIndexUpdater.INSTANCE;
    }

    @Override
    public void force()
    {
    }

    @Override
    public void flush()
    {
    }

    @Override
    public NewIndexDescriptor getDescriptor()
    {
        return descriptor;
    }

    @Override
    public SchemaIndexProvider.Descriptor getProviderDescriptor()
    {
        return providerDescriptor;
    }

    @Override
    public Future<Void> close()
    {
        return VOID;
    }

    @Override
    public IndexReader newReader()
    {
        throw new UnsupportedOperationException();
    }
}

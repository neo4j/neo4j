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
package org.neo4j.kernel.impl.api.index;

import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.updater.SwallowingIndexUpdater;
import org.neo4j.storageengine.api.schema.CapableIndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.PopulationProgress;

public abstract class AbstractSwallowingIndexProxy implements IndexProxy
{
    private final CapableIndexDescriptor capableIndexDescriptor;
    private final IndexPopulationFailure populationFailure;

    AbstractSwallowingIndexProxy( CapableIndexDescriptor capableIndexDescriptor, IndexPopulationFailure populationFailure )
    {
        this.capableIndexDescriptor = capableIndexDescriptor;
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
    public void force( IOLimiter ioLimiter )
    {
    }

    @Override
    public void refresh()
    {
    }

    @Override
    public CapableIndexDescriptor getDescriptor()
    {
        return capableIndexDescriptor;
    }

    @Override
    public void close()
    {
    }

    @Override
    public IndexReader newReader()
    {
        throw new UnsupportedOperationException();
    }
}

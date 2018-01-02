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
import java.util.concurrent.Future;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.api.index.IndexConfiguration;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.SchemaIndexProvider;

import static org.neo4j.helpers.FutureAdapter.VOID;
import static org.neo4j.helpers.collection.IteratorUtil.emptyIterator;

public class IndexProxyAdapter implements IndexProxy
{
    @Override
    public void start()
    {
    }

    @Override
    public IndexUpdater newUpdater( IndexUpdateMode mode )
    {
        return SwallowingIndexUpdater.INSTANCE;
    }

    @Override
    public Future<Void> drop()
    {
        return VOID;
    }

    @Override
    public InternalIndexState getState()
    {
        throw new UnsupportedOperationException();
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
    public Future<Void> close()
    {
        return VOID;
    }

    @Override
    public IndexDescriptor getDescriptor()
    {
        return null;
    }

    @Override
    public SchemaIndexProvider.Descriptor getProviderDescriptor()
    {
        return null;
    }

    @Override
    public IndexReader newReader()
    {
        return IndexReader.EMPTY;
    }

    @Override
    public boolean awaitStoreScanCompleted()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void activate()
    {
    }

    @Override
    public void validate()
    {
    }

    @Override
    public ResourceIterator<File> snapshotFiles()
    {
        return emptyIterator();
    }

    @Override
    public IndexConfiguration config()
    {
        return null;
    }

    @Override
    public IndexPopulationFailure getPopulationFailure() throws IllegalStateException
    {
        throw new IllegalStateException( "This index isn't failed" );
    }
}

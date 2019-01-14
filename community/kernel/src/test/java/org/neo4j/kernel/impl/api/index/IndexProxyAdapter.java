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

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.updater.SwallowingIndexUpdater;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.PopulationProgress;
import org.neo4j.values.storable.Value;

import static org.neo4j.helpers.collection.Iterators.emptyResourceIterator;

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
    public void drop()
    {
    }

    @Override
    public InternalIndexState getState()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexCapability getIndexCapability()
    {
        return null;
    }

    public void force( IOLimiter ioLimiter )
    {
    }

    @Override
    public void refresh()
    {
    }

    @Override
    public void close()
    {
    }

    @Override
    public SchemaIndexDescriptor getDescriptor()
    {
        return null;
    }

    @Override
    public LabelSchemaDescriptor schema()
    {
        return null;
    }

    @Override
    public IndexProvider.Descriptor getProviderDescriptor()
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
    public void validateBeforeCommit( Value[] tuple )
    {
    }

    @Override
    public long getIndexId()
    {
        return 1;
    }

    @Override
    public ResourceIterator<File> snapshotFiles()
    {
        return emptyResourceIterator();
    }

    @Override
    public IndexPopulationFailure getPopulationFailure() throws IllegalStateException
    {
        throw new IllegalStateException( "This index isn't failed" );
    }

    @Override
    public PopulationProgress getIndexPopulationProgress()
    {
        return PopulationProgress.NONE;
    }
}

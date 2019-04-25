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
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.api.exceptions.index.IndexActivationFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.storageengine.api.schema.CapableIndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.PopulationProgress;
import org.neo4j.values.storable.Value;

public abstract class AbstractDelegatingIndexProxy implements IndexProxy
{
    protected abstract IndexProxy getDelegate();

    @Override
    public void start()
    {
        getDelegate().start();
    }

    @Override
    public IndexUpdater newUpdater( IndexUpdateMode mode )
    {
        return getDelegate().newUpdater( mode );
    }

    @Override
    public void drop()
    {
        getDelegate().drop();
    }

    @Override
    public InternalIndexState getState()
    {
        return getDelegate().getState();
    }

    @Override
    public CapableIndexDescriptor getDescriptor()
    {
        return getDelegate().getDescriptor();
    }

    @Override
    public void force( IOLimiter ioLimiter ) throws IOException
    {
        getDelegate().force( ioLimiter );
    }

    @Override
    public void refresh() throws IOException
    {
        getDelegate().refresh();
    }

    @Override
    public void close() throws IOException
    {
        getDelegate().close();
    }

    @Override
    public IndexReader newReader() throws IndexNotFoundKernelException
    {
        return getDelegate().newReader();
    }

    @Override
    public boolean awaitStoreScanCompleted( long time, TimeUnit unit ) throws IndexPopulationFailedKernelException, InterruptedException
    {
        return getDelegate().awaitStoreScanCompleted( time, unit );
    }

    @Override
    public void activate() throws IndexActivationFailedKernelException
    {
        getDelegate().activate();
    }

    @Override
    public void validate() throws IndexPopulationFailedKernelException, UniquePropertyValueValidationException
    {
        getDelegate().validate();
    }

    @Override
    public void validateBeforeCommit( Value[] tuple )
    {
        getDelegate().validateBeforeCommit( tuple );
    }

    @Override
    public IndexPopulationFailure getPopulationFailure() throws IllegalStateException
    {
        return getDelegate().getPopulationFailure();
    }

    @Override
    public PopulationProgress getIndexPopulationProgress()
    {
        return getDelegate().getIndexPopulationProgress();
    }

    @Override
    public String toString()
    {
        return String.format( "%s -> %s", getClass().getSimpleName(), getDelegate().toString() );
    }

    @Override
    public ResourceIterator<File> snapshotFiles() throws IOException
    {
        return getDelegate().snapshotFiles();
    }

    @Override
    public Map<String,Value> indexConfig()
    {
        return getDelegate().indexConfig();
    }

    @Override
    public void verifyDeferredConstraints( NodePropertyAccessor accessor ) throws IndexEntryConflictException, IOException
    {
        getDelegate().verifyDeferredConstraints( accessor );
    }
}

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
package org.neo4j.kernel.impl.api.index.inmemory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.storageengine.api.schema.IndexReader;

public class UpdateCapturingIndexAccessor implements IndexAccessor
{
    private final IndexAccessor actual;
    private final Collection<IndexEntryUpdate<?>> updates = new ArrayList<>();

    public UpdateCapturingIndexAccessor( IndexAccessor actual, Collection<IndexEntryUpdate<?>> initialUpdates )
    {
        this.actual = actual;
        if ( initialUpdates != null )
        {
            this.updates.addAll( initialUpdates );
        }
    }

    @Override
    public void drop() throws IOException
    {
        actual.drop();
    }

    @Override
    public IndexUpdater newUpdater( IndexUpdateMode mode )
    {
        return wrap( actual.newUpdater( mode ) );
    }

    private IndexUpdater wrap( IndexUpdater actual )
    {
        return new UpdateCapturingIndexUpdater( actual, updates );
    }

    @Override
    public void force() throws IOException
    {
        actual.force();
    }

    @Override
    public void close() throws IOException
    {
        actual.close();
    }

    @Override
    public IndexReader newReader()
    {
        return actual.newReader();
    }

    @Override
    public BoundedIterable<Long> newAllEntriesReader()
    {
        return actual.newAllEntriesReader();
    }

    @Override
    public ResourceIterator<File> snapshotFiles() throws IOException
    {
        return actual.snapshotFiles();
    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor propertyAccessor ) throws IndexEntryConflictException, IOException
    {
        actual.verifyDeferredConstraints( propertyAccessor );
    }

    public Collection<IndexEntryUpdate<?>> snapshot()
    {
        return new ArrayList<>( updates );
    }
}

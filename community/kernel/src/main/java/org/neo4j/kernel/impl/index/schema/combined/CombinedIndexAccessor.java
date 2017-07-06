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
package org.neo4j.kernel.impl.index.schema.combined;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.storageengine.api.schema.IndexReader;

import static java.util.Arrays.asList;
import static org.neo4j.helpers.collection.Iterators.concatResourceIterators;

class CombinedIndexAccessor implements IndexAccessor
{
    private final IndexAccessor boostAccessor;
    private final IndexAccessor fallbackAccessor;

    CombinedIndexAccessor( IndexAccessor boostAccessor, IndexAccessor fallbackAccessor )
    {
        this.boostAccessor = boostAccessor;
        this.fallbackAccessor = fallbackAccessor;
    }

    @Override
    public void drop() throws IOException
    {
        try
        {
            boostAccessor.drop();
        }
        finally
        {
            fallbackAccessor.drop();
        }
    }

    @Override
    public IndexUpdater newUpdater( IndexUpdateMode mode )
    {
        return new CombinedIndexUpdater( boostAccessor.newUpdater( mode ), fallbackAccessor.newUpdater( mode ) );
    }

    @Override
    public void force() throws IOException
    {
        boostAccessor.force();
        fallbackAccessor.force();
    }

    @Override
    public void close() throws IOException
    {
        try
        {
            boostAccessor.close();
        }
        finally
        {
            fallbackAccessor.close();
        }
    }

    @Override
    public IndexReader newReader()
    {
        return new CombinedIndexReader( boostAccessor.newReader(), fallbackAccessor.newReader() );
    }

    @Override
    public BoundedIterable<Long> newAllEntriesReader()
    {
        return null;
    }

    @Override
    public ResourceIterator<File> snapshotFiles() throws IOException
    {
        return concatResourceIterators(
                asList( boostAccessor.snapshotFiles(), fallbackAccessor.snapshotFiles() ).iterator() );
    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor propertyAccessor )
            throws IndexEntryConflictException, IOException
    {
        boostAccessor.verifyDeferredConstraints( propertyAccessor );
        fallbackAccessor.verifyDeferredConstraints( propertyAccessor );
    }
}

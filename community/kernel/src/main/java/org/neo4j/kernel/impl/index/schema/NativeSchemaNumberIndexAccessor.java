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
package org.neo4j.kernel.impl.index.schema;

import java.io.File;
import java.io.IOException;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.IndexQuery;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSampler;

public class NativeSchemaNumberIndexAccessor implements IndexAccessor
{
    @Override
    public void drop() throws IOException
    {
        throw new UnsupportedOperationException( "Implement me" );
    }

    @Override
    public IndexUpdater newUpdater( IndexUpdateMode mode )
    {
        return new NativeSchemaNumberIndexUpdater();
    }

    @Override
    public void flush() throws IOException
    {
        throw new UnsupportedOperationException( "Implement me" );
    }

    @Override
    public void force() throws IOException
    {
        throw new UnsupportedOperationException( "Implement me" );
    }

    @Override
    public void close() throws IOException
    {
        throw new UnsupportedOperationException( "Implement me" );
    }

    @Override
    public IndexReader newReader()
    {
        return new NativeSchemaNumberIndexReader();
    }

    @Override
    public BoundedIterable<Long> newAllEntriesReader()
    {
        throw new UnsupportedOperationException( "Implement me" );
    }

    @Override
    public ResourceIterator<File> snapshotFiles() throws IOException
    {
        throw new UnsupportedOperationException( "Implement me" );
    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor propertyAccessor )
            throws IndexEntryConflictException, IOException
    {
        throw new UnsupportedOperationException( "Implement me" );
    }

    private class NativeSchemaNumberIndexUpdater implements IndexUpdater
    {

        @Override
        public void process( IndexEntryUpdate update ) throws IOException, IndexEntryConflictException
        {
            throw new UnsupportedOperationException( "Implement me" );
        }

        @Override
        public void close() throws IOException, IndexEntryConflictException
        {
            throw new UnsupportedOperationException( "Implement me" );
        }

        @Override
        public void remove( PrimitiveLongSet nodeIds ) throws IOException
        {
            throw new UnsupportedOperationException( "Implement me" );
        }
    }

    private class NativeSchemaNumberIndexReader implements IndexReader
    {

        @Override
        public void close()
        {
            throw new UnsupportedOperationException( "Implement me" );
        }

        @Override
        public long countIndexedNodes( long nodeId, Object... propertyValues )
        {
            throw new UnsupportedOperationException( "Implement me" );
        }

        @Override
        public IndexSampler createSampler()
        {
            throw new UnsupportedOperationException( "Implement me" );
        }

        @Override
        public PrimitiveLongIterator query( IndexQuery... predicates ) throws IndexNotApplicableKernelException
        {
            throw new UnsupportedOperationException( "Implement me" );
        }
    }
}

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
package org.neo4j.kernel.impl.api.index.inmemory;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.values.storable.Value;

import static org.neo4j.helpers.collection.Iterators.emptyResourceIterator;

class InMemoryIndex
{
    final InMemoryIndexImplementation indexData;
    private InternalIndexState state = InternalIndexState.POPULATING;
    String failure;

    InMemoryIndex( SchemaIndexDescriptor descriptor )
    {
        this( new HashBasedIndex( descriptor ) );
    }

    private InMemoryIndex( InMemoryIndexImplementation indexData )
    {
        this.indexData = indexData;
    }

    @Override
    public String toString()
    {
        if ( failure != null )
        {
            return String.format( "%s[failure=\"%s\"]%s", getClass().getSimpleName(), failure, indexData );
        }
        else
        {
            return String.format( "%s%s", getClass().getSimpleName(), indexData );
        }
    }

    final IndexPopulator getPopulator()
    {
        return new Populator();
    }

    final IndexAccessor getOnlineAccessor()
    {
        return new OnlineAccessor();
    }

    protected boolean add( long nodeId, Value[] propertyValues, boolean applyIdempotently )
    {
        assert propertyValues.length > 0;
        return indexData.add( nodeId, applyIdempotently, propertyValues );
    }

    protected void remove( long nodeId, Value[] propertyValues )
    {
        assert propertyValues.length > 0;
        indexData.remove( nodeId, propertyValues );
    }

    protected void remove( long nodeId )
    {
        indexData.remove( nodeId );
    }

    InternalIndexState getState()
    {
        return state;
    }

    private class Populator implements IndexPopulator
    {
        private Populator()
        {
        }

        @Override
        public void create()
        {
            indexData.initialize();
        }

        @Override
        public void add( Collection<? extends IndexEntryUpdate<?>> updates )
        {
            for ( IndexEntryUpdate<?> update : updates )
            {
                InMemoryIndex.this.add( update.getEntityId(), update.values(), false );
            }
        }

        @Override
        public void verifyDeferredConstraints( PropertyAccessor accessor ) throws IndexEntryConflictException
        {
            InMemoryIndex.this.verifyDeferredConstraints( accessor );
        }

        @Override
        public IndexUpdater newPopulatingUpdater( PropertyAccessor propertyAccessor )
        {
            return InMemoryIndex.this.newUpdater( IndexUpdateMode.ONLINE, true );
        }

        @Override
        public void drop()
        {
            indexData.drop();
        }

        @Override
        public void close( boolean populationCompletedSuccessfully )
        {
            if ( populationCompletedSuccessfully )
            {
                state = InternalIndexState.ONLINE;
            }
        }

        @Override
        public void markAsFailed( String failureString )
        {
            failure = failureString;
            state = InternalIndexState.FAILED;
        }

        @Override
        public void includeSample( IndexEntryUpdate<?> update )
        {
        }

        @Override
        public IndexSample sampleResult()
        {
            try
            {
                return indexData.createSampler().sampleIndex();
            }
            catch ( IndexNotFoundKernelException e )
            {
                throw new IllegalStateException( e );
            }
        }
    }

    public void verifyDeferredConstraints( PropertyAccessor accessor ) throws IndexEntryConflictException
    {
    }

    private class OnlineAccessor implements IndexAccessor
    {
        @Override
        public void force( IOLimiter ioLimiter )
        {
        }

        @Override
        public void refresh()
        {
        }

        @Override
        public void drop()
        {
            indexData.drop();
        }

        @Override
        public IndexUpdater newUpdater( final IndexUpdateMode mode )
        {
            return InMemoryIndex.this.newUpdater( mode, false );
        }

        @Override
        public void close()
        {
        }

        @Override
        public IndexReader newReader()
        {
            return indexData;
        }

        @Override
        public BoundedIterable<Long> newAllEntriesReader()
        {
            return indexData;
        }

        @Override
        public ResourceIterator<File> snapshotFiles()
        {
            return emptyResourceIterator();
        }

        @Override
        public void verifyDeferredConstraints( PropertyAccessor propertyAccessor )
                throws IndexEntryConflictException
        {
            InMemoryIndex.this.verifyDeferredConstraints( propertyAccessor );
        }

        @Override
        public boolean isDirty()
        {
            return false;
        }
    }

    protected IndexUpdater newUpdater( IndexUpdateMode mode, boolean populating )
    {
        return new InMemoryIndexUpdater( populating );
    }

    private class InMemoryIndexUpdater implements IndexUpdater
    {
        private final boolean applyIdempotently;

        private InMemoryIndexUpdater( boolean applyIdempotently )
        {
            this.applyIdempotently = applyIdempotently;
        }

        @Override
        public void process( IndexEntryUpdate<?> update )
        {
            switch ( update.updateMode() )
            {
            case ADDED:
                InMemoryIndex.this.add( update.getEntityId(), update.values(), applyIdempotently );
                break;
            case CHANGED:
                InMemoryIndex.this.remove( update.getEntityId(), update.beforeValues() );
                add( update.getEntityId(), update.values(), applyIdempotently );
                break;
            case REMOVED:
                InMemoryIndex.this.remove( update.getEntityId(), update.values() );
                break;
            default:
                throw new UnsupportedOperationException();
            }
        }

        @Override
        public void close()
        {
        }
    }

    InMemoryIndex snapshot()
    {
        InMemoryIndex snapshot = new InMemoryIndex( indexData.snapshot() );
        snapshot.failure = this.failure;
        snapshot.state = this.state;
        return snapshot;
    }

    static String encodeAsString( Object propertyValue )
    {
        String repr;
        if ( propertyValue instanceof int[] )
        {
            repr = Arrays.toString( (int[]) propertyValue );
        }
        else if ( propertyValue instanceof long[] )
        {
            repr = Arrays.toString( (long[]) propertyValue );
        }
        else if ( propertyValue instanceof boolean[] )
        {
            repr = Arrays.toString( (boolean[]) propertyValue );
        }
        else if ( propertyValue instanceof double[] )
        {
            repr = Arrays.toString( (double[]) propertyValue );
        }
        else if ( propertyValue instanceof float[] )
        {
            repr = Arrays.toString( (float[]) propertyValue );
        }
        else if ( propertyValue instanceof short[] )
        {
            repr = Arrays.toString( (short[]) propertyValue );
        }
        else if ( propertyValue instanceof byte[] )
        {
            repr = Arrays.toString( (byte[]) propertyValue );
        }
        else if ( propertyValue instanceof char[] )
        {
            repr = Arrays.toString( (char[]) propertyValue );
        }
        else if ( propertyValue instanceof Object[] )
        {
            repr = Arrays.toString( (Object[]) propertyValue );
        }
        else
        {
            repr = propertyValue.toString();
        }
        return repr;
    }

    boolean hasSameContentsAs( InMemoryIndex otherIndex )
    {
        return indexData.hasSameContentsAs( otherIndex.indexData );
    }
}

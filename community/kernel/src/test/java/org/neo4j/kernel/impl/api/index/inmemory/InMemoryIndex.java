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
package org.neo4j.kernel.impl.api.index.inmemory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.collection.primitive.PrimitiveLongVisitor;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.api.direct.BoundedIterable;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.index.Reservation;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.register.Register.DoubleLong;

import static org.neo4j.helpers.collection.IteratorUtil.emptyIterator;

class InMemoryIndex
{
    protected final InMemoryIndexImplementation indexData;
    private InternalIndexState state = InternalIndexState.POPULATING;
    String failure;

    private final PrimitiveLongVisitor<RuntimeException> removeFromIndex = new PrimitiveLongVisitor<RuntimeException>()
    {
        @Override
        public boolean visited( long nodeId ) throws RuntimeException
        {
            indexData.remove( nodeId );
            return false;
        }
    };

    InMemoryIndex()
    {
        this( new HashBasedIndex() );
    }

    InMemoryIndex( InMemoryIndexImplementation indexData )
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

    protected final PrimitiveLongIterator indexSeek( Object propertyValue )
    {
        return indexData.seek( propertyValue );
    }

    protected boolean add( long nodeId, Object propertyValue, boolean applyIdempotently )
            throws IndexEntryConflictException, IOException
    {
        return indexData.add( nodeId, propertyValue, applyIdempotently );
    }

    protected void remove( long nodeId, Object propertyValue )
    {
        indexData.remove( nodeId, propertyValue );
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
        public void add( long nodeId, Object propertyValue ) throws IndexEntryConflictException, IOException
        {
            InMemoryIndex.this.add( nodeId, propertyValue, false );
        }

        @Override
        public void verifyDeferredConstraints( PropertyAccessor accessor ) throws IndexEntryConflictException, IOException
        {
            InMemoryIndex.this.verifyDeferredConstraints( accessor );
        }

        @Override
        public IndexUpdater newPopulatingUpdater( PropertyAccessor propertyAccessor ) throws IOException
        {
            return InMemoryIndex.this.newUpdater( IndexUpdateMode.ONLINE, true );
        }

        @Override
        public void drop() throws IOException
        {
            indexData.drop();
        }

        @Override
        public void close( boolean populationCompletedSuccessfully ) throws IOException
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
        public long sampleResult( DoubleLong.Out result )
        {
            try
            {
                return indexData.sampleIndex( result );
            }
            catch ( IndexNotFoundKernelException e )
            {
                throw new IllegalStateException( e );
            }
        }
    }

    public void verifyDeferredConstraints( PropertyAccessor accessor ) throws IndexEntryConflictException, IOException
    {
    }

    private class OnlineAccessor implements IndexAccessor
    {
        @Override
        public void force()
        {
        }

        @Override
        public void flush()
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
            return emptyIterator();
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
        public Reservation validate( Iterable<NodePropertyUpdate> updates ) throws IOException
        {
            return Reservation.EMPTY;
        }

        @Override
        public void process( NodePropertyUpdate update ) throws IOException, IndexEntryConflictException
        {
            switch ( update.getUpdateMode() )
            {
            case ADDED:
                InMemoryIndex.this.add( update.getNodeId(), update.getValueAfter(), applyIdempotently );
                break;
            case CHANGED:
                InMemoryIndex.this.remove( update.getNodeId(), update.getValueBefore() );
                add( update.getNodeId(), update.getValueAfter(), applyIdempotently );
                break;
            case REMOVED:
                InMemoryIndex.this.remove( update.getNodeId(), update.getValueBefore() );
                break;
            default:
                throw new UnsupportedOperationException();
            }
        }

        @Override
        public void close() throws IOException, IndexEntryConflictException
        {
        }

        @Override
        public void remove( PrimitiveLongSet nodeIds )
        {
            nodeIds.visitKeys( removeFromIndex );
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
}

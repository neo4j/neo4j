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
package org.neo4j.kernel.impl.newapi;

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.LongSet;

import java.util.function.Consumer;

import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.TokenSet;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.IndexProgressor.EntityTokenClient;
import org.neo4j.kernel.api.txstate.TransactionState;

import static org.neo4j.collection.PrimitiveLongCollections.iterator;
import static org.neo4j.collection.PrimitiveLongCollections.reverseIterator;
import static org.neo4j.internal.schema.IndexOrder.DESCENDING;
import static org.neo4j.kernel.impl.newapi.Read.NO_ID;

/**
 * Base for index cursors that can handle scans with IndexOrder.
 */
abstract class DefaultEntityTokenIndexCursor<SELF extends DefaultEntityTokenIndexCursor<SELF>> extends IndexCursor<IndexProgressor,SELF>
{
    private Read read;
    private long entity;
    private TokenSet tokens;
    private LongIterator added;
    private LongSet removed;
    private boolean useMergeSort;
    private final PrimitiveSortedMergeJoin sortedMergeJoin = new PrimitiveSortedMergeJoin();

    private AccessMode accessMode;
    private boolean shortcutSecurity;

    DefaultEntityTokenIndexCursor( CursorPool<SELF> pool )
    {
        super( pool );
        this.entity = NO_ID;
    }

    /**
     * The returned LongSets must be immutable or a private copy.
     */
    abstract LongSet createAddedInTxState( TransactionState txState, int token );
    abstract LongSet createDeletedInTxState( TransactionState txState, int token );

    abstract void traceScan( KernelReadTracer tracer, int token );
    abstract void traceNext( KernelReadTracer tracer, long entity );
    abstract boolean allowedToSeeAllEntitiesWithToken( AccessMode accessMode, int token );
    abstract boolean allowedToSeeEntity( AccessMode accessMode, long entityReference, TokenSet tokens );

    public void scan( IndexProgressor progressor, int token, IndexOrder order )
    {
        initialize( progressor );
        if ( read.hasTxStateWithChanges() )
        {
            LongSet frozenAdded = createAddedInTxState( read.txState(), token );
            switch ( order )
            {
            case NONE:
                useMergeSort = false;
                added = frozenAdded.longIterator();
                break;
            case ASCENDING:
            case DESCENDING:
                useMergeSort = true;
                sortedMergeJoin.initialize( order );
                long[] addedSortedArray = frozenAdded.toSortedArray();
                added = DESCENDING == order ? reverseIterator( addedSortedArray ) : iterator( addedSortedArray );
                break;
            default:
                throw new IllegalArgumentException( "Unsupported index order:" + order );
            }
            removed = createDeletedInTxState( read.txState(), token );
        }
        else
        {
            useMergeSort = false;
        }

        if ( tracer != null )
        {
            traceScan( tracer, token );
        }
        initSecurity( token );
    }

    public void scan( IndexProgressor progressor, LongIterator added, LongSet removed, int token )
    {
        initialize( progressor );
        useMergeSort = false;
        this.added = added;
        this.removed = removed;
        initSecurity( token );
    }

    EntityTokenClient entityTokenClient()
    {
        return ( reference, tokens ) ->
        {
            if ( isRemoved( reference ) || !allowed( reference, tokens ) )
            {
                return false;
            }
            else
            {
                DefaultEntityTokenIndexCursor.this.entity = reference;
                DefaultEntityTokenIndexCursor.this.tokens = tokens;

                return true;
            }
        };
    }

    private void initSecurity( int token )
    {
        if ( accessMode == null )
        {
            accessMode = read.ktx.securityContext().mode();
        }
        shortcutSecurity = allowedToSeeAllEntitiesWithToken( accessMode, token );
    }

    boolean allowed( long reference, TokenSet tokens )
    {
        if ( shortcutSecurity )
        {
            return true;
        }
        if ( accessMode == null )
        {
            accessMode = read.ktx.securityContext().mode();
        }
        return allowedToSeeEntity( accessMode, reference, tokens );
    }

    @Override
    public boolean next()
    {
        if ( useMergeSort )
        {
            return nextWithOrdering();
        }
        else
        {
            return nextWithoutOrder();
        }
    }

    private boolean nextWithoutOrder()
    {
        if ( added != null && added.hasNext() )
        {
            this.entity = added.next();
            if ( tracer != null )
            {
                traceNext( tracer, this.entity );
            }
            return true;
        }
        else
        {
            boolean hasNext = innerNext();
            if ( tracer != null && hasNext )
            {
                traceNext( tracer, this.entity );
            }
            return hasNext;
        }
    }

    private boolean nextWithOrdering()
    {
        if ( sortedMergeJoin.needsA() && added.hasNext() )
        {
            long entity = added.next();
            sortedMergeJoin.setA( entity );
        }

        if ( sortedMergeJoin.needsB() && innerNext() )
        {
            sortedMergeJoin.setB( this.entity );
        }

        this.entity = sortedMergeJoin.next();
        boolean next = this.entity != -1;
        if ( tracer != null && next )
        {
            traceNext( tracer, this.entity );
        }
        return next;
    }

    public void setRead( Read read )
    {
        this.read = read;
    }

    public long entityReference()
    {
        return entity;
    }

    protected void readEntity( Consumer<Read> entityReader )
    {
        entityReader.accept( read );
    }

    public TokenSet tokens()
    {
        return tokens;
    }

    @Override
    public void closeInternal()
    {
        if ( !isClosed() )
        {
            closeProgressor();
            entity = NO_ID;
            tokens = null;
            read = null;
            added = null;
            removed = null;
            accessMode = null;
        }
        super.closeInternal();
    }

    @Override
    public boolean isClosed()
    {
        return isProgressorClosed();
    }

    private boolean isRemoved( long reference )
    {
        return removed != null && removed.contains( reference );
    }
}

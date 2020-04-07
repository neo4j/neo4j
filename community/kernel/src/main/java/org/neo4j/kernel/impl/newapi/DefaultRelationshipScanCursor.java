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
import org.eclipse.collections.impl.iterator.ImmutableEmptyLongIterator;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.api.AllRelationshipsScan;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;

import static org.neo4j.kernel.impl.newapi.Read.NO_ID;

class DefaultRelationshipScanCursor extends DefaultRelationshipCursor<StorageRelationshipScanCursor> implements RelationshipScanCursor
{
    private int type;
    private long single;
    private LongIterator addedRelationships;
    private CursorPool<DefaultRelationshipScanCursor> pool;
    private final PageCursorTracer cursorTracer;

    DefaultRelationshipScanCursor( CursorPool<DefaultRelationshipScanCursor> pool, StorageRelationshipScanCursor storeCursor, PageCursorTracer cursorTracer )
    {
        super( storeCursor );
        this.pool = pool;
        this.cursorTracer = cursorTracer;
    }

    void scan( int type, Read read )
    {
        storeCursor.scan( type );
        this.type = type;
        this.single = NO_ID;
        init( read );
        this.addedRelationships = ImmutableEmptyLongIterator.INSTANCE;
    }

    boolean scanBatch( Read read, AllRelationshipsScan scan, int sizeHint, LongIterator addedRelationships, boolean hasChanges )
    {
        this.read = read;
        this.single = NO_ID;
        this.type = -1;
        this.currentAddedInTx = NO_ID;
        this.addedRelationships = addedRelationships;
        this.hasChanges = hasChanges;
        this.checkHasChanges = false;
        boolean scanBatch = storeCursor.scanBatch( scan, sizeHint );
        return addedRelationships.hasNext() || scanBatch;
    }

    void single( long reference, Read read )
    {
        storeCursor.single( reference );
        type = -1;
        this.single = reference;
        init( read );
        this.addedRelationships = ImmutableEmptyLongIterator.INSTANCE;
    }

    @Override
    public boolean next()
    {
        // Check tx state
        boolean hasChanges = hasChanges();

        if ( hasChanges )
        {
            if ( addedRelationships.hasNext() )
            {
                read.txState().relationshipVisit( addedRelationships.next(), relationshipTxStateDataVisitor );
                if ( tracer != null )
                {
                    tracer.onRelationship( relationshipReference() );
                }
                return true;
            }
            else
            {
                currentAddedInTx = NO_ID;
            }
        }

        while ( storeCursor.next() )
        {
            boolean skip = hasChanges && read.txState().relationshipIsDeletedInThisTx( storeCursor.entityReference() );
            if ( !skip && allowed() )
            {
                if ( tracer != null )
                {
                    tracer.onRelationship( relationshipReference() );
                }
                return true;
            }
        }
        return false;
    }

    boolean allowed()
    {
        AccessMode mode = read.ktx.securityContext().mode();
        return mode.allowsTraverseRelType( storeCursor.type() ) && allowedToSeeEndNode( mode );
    }

    private boolean allowedToSeeEndNode( AccessMode mode )
    {
        if ( mode.allowsTraverseAllLabels() )
        {
            return true;
        }
        try ( NodeCursor sourceNode = read.cursors().allocateNodeCursor( cursorTracer );
              NodeCursor targetNode = read.cursors().allocateNodeCursor( cursorTracer ) )
        {
            read.singleNode( storeCursor.sourceNodeReference(), sourceNode );
            read.singleNode( storeCursor.targetNodeReference(), targetNode );
            return sourceNode.next() && targetNode.next();
        }
    }

    @Override
    public void closeInternal()
    {
        if ( !isClosed() )
        {
            read = null;
            storeCursor.close();

            pool.accept( this );
        }
    }

    @Override
    public boolean isClosed()
    {
        return read == null;
    }

    @Override
    public String toString()
    {
        if ( isClosed() )
        {
            return "RelationshipScanCursor[closed state]";
        }
        else
        {
            return "RelationshipScanCursor[id=" + storeCursor.entityReference() +
                    ", open state with: single=" + single +
                    ", type=" + type +
                    ", " + storeCursor + "]";
        }
    }

    @Override
    protected void collectAddedTxStateSnapshot()
    {
        if ( isSingle() )
        {
            addedRelationships = read.txState().relationshipIsAddedInThisTx( single )
                                 ? LongHashSet.newSetWith( single ).longIterator()
                                 : ImmutableEmptyLongIterator.INSTANCE;
        }
        else
        {
            addedRelationships = read.txState().addedAndRemovedRelationships().getAdded().longIterator();
        }
    }

    private boolean isSingle()
    {
        return single != NO_ID;
    }

    public void release()
    {
        storeCursor.close();
    }
}

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
package org.neo4j.internal.recordstorage;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.RecordLoadOverride;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.neo4j.kernel.impl.store.record.RecordLoad.ALWAYS;

class RecordRelationshipGroupCursor extends RelationshipGroupRecord implements AutoCloseable
{
    private final RelationshipStore relationshipStore;
    private final RelationshipGroupStore groupStore;
    private final PageCursorTracer cursorTracer;
    private final RelationshipRecord edge = new RelationshipRecord( NO_ID );

    private PageCursor page;
    private PageCursor edgePage;
    private boolean open;
    RecordLoadOverride loadMode;

    RecordRelationshipGroupCursor( RelationshipStore relationshipStore, RelationshipGroupStore groupStore, PageCursorTracer cursorTracer,
            RecordLoadOverride loadMode )
    {
        super( NO_ID );
        this.relationshipStore = relationshipStore;
        this.groupStore = groupStore;
        this.cursorTracer = cursorTracer;
        this.loadMode = loadMode;
    }

    void init( long nodeReference, long reference, boolean nodeIsDense )
    {
        // the relationships for this node are not grouped in the store
        if ( reference != NO_ID && !nodeIsDense )
        {
            throw new UnsupportedOperationException( "Not a dense node" );
        }
        else // this is a normal group reference.
        {
            direct( nodeReference, reference );
        }
        open = true;
    }

    /**
     * Dense node, real groups iterated with every call to next.
     */
    void direct( long nodeReference, long reference )
    {
        clear();
        setOwningNode( nodeReference );
        setNext( reference );
        if ( page == null )
        {
            page = groupPage( reference );
        }
    }

    boolean next()
    {
        do
        {
            if ( getNext() == NO_ID )
            {
                //We have now run out of groups from the store, however there may still
                //be new types that was added in the transaction that we haven't visited yet.
                return false;
            }
            group( this, getNext(), page );
        } while ( !inUse() );

        return true;
    }

    int outgoingCount()
    {
        return count( outgoingRawId() );
    }

    int incomingCount()
    {
        return count( incomingRawId() );
    }

    int loopCount()
    {
        return count( loopsRawId() );
    }

    private int count( long reference )
    {
        if ( reference == NO_ID )
        {
            return 0;
        }
        if ( edgePage == null )
        {
            edgePage = relationshipStore.openPageCursorForReading( reference, cursorTracer );
        }
        relationshipStore.getRecordByCursor( reference, edge, loadMode.orElse( ALWAYS ), edgePage );
        if ( edge.getFirstNode() == getOwningNode() )
        {
            return (int) edge.getFirstPrevRel();
        }
        else
        {
            return (int) edge.getSecondPrevRel();
        }
    }

    @Override
    public RelationshipGroupRecord copy()
    {
        throw new UnsupportedOperationException( "Record cursors are not copyable." );
    }

    @Override
    public String toString()
    {
        if ( !open )
        {
            return "RelationshipGroupCursor[closed state]";
        }
        else
        {
            return "RelationshipGroupCursor[id=" + getId() + ", open state with: underlying record=" + super.toString() + "]";
        }
    }

    /**
     * Implementation detail which provides the raw non-encoded outgoing relationship id
     */
    long outgoingRawId()
    {
        return getFirstOut();
    }

    /**
     * Implementation detail which provides the raw non-encoded incoming relationship id
     */
    long incomingRawId()
    {
        return getFirstIn();
    }

    /**
     * Implementation detail which provides the raw non-encoded loops relationship id
     */
    long loopsRawId()
    {
        return getFirstLoop();
    }

    @Override
    public void close()
    {
        if ( edgePage != null )
        {
            edgePage.close();
            edgePage = null;
        }

        if ( page != null )
        {
            page.close();
            page = null;
        }
    }

    private PageCursor groupPage( long reference )
    {
        return groupStore.openPageCursorForReading( reference, cursorTracer );
    }

    private void group( RelationshipGroupRecord record, long reference, PageCursor page )
    {
        // We need to load forcefully here since otherwise we cannot traverse over groups
        // records which have been concurrently deleted (flagged as inUse = false).
        // @see #org.neo4j.kernel.impl.store.RelationshipChainPointerChasingTest
        groupStore.getRecordByCursor( reference, record, loadMode.orElse( ALWAYS ), page );
    }
}

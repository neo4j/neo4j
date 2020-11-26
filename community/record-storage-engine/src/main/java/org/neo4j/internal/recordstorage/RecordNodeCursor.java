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

import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RecordLoadOverride;
import org.neo4j.storageengine.api.AllNodeScan;
import org.neo4j.storageengine.api.Degrees;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;

import static java.lang.Math.min;
import static org.neo4j.internal.recordstorage.RelationshipReferenceEncoding.encodeDense;
import static org.neo4j.storageengine.api.RelationshipSelection.ALL_RELATIONSHIPS;

public class RecordNodeCursor extends NodeRecord implements StorageNodeCursor
{
    private final NodeStore read;
    private final PageCursorTracer cursorTracer;
    private final RelationshipStore relationshipStore;
    private final RelationshipGroupStore groupStore;
    private PageCursor pageCursor;
    private long next;
    private long highMark;
    private long nextStoreReference;
    private boolean open;
    private boolean batched;
    private RecordRelationshipGroupCursor groupCursor;
    private RecordRelationshipTraversalCursor relationshipCursor;
    private RecordRelationshipScanCursor relationshipScanCursor;
    private RecordLoadOverride loadMode;

    RecordNodeCursor( NodeStore read, RelationshipStore relationshipStore, RelationshipGroupStore groupStore, PageCursorTracer cursorTracer )
    {
        super( NO_ID );
        this.read = read;
        this.cursorTracer = cursorTracer;
        this.relationshipStore = relationshipStore;
        this.groupStore = groupStore;
        this.loadMode = RecordLoadOverride.none();
    }

    @Override
    public void scan()
    {
        if ( getId() != NO_ID )
        {
            resetState();
        }
        if ( pageCursor == null )
        {
            pageCursor = nodePage( 0 );
        }
        this.next = 0;
        this.highMark = nodeHighMark();
        this.nextStoreReference = NO_ID;
        this.open = true;
        this.batched = false;
    }

    @Override
    public void single( long reference )
    {
        if ( getId() != NO_ID )
        {
            resetState();
        }
        if ( pageCursor == null )
        {
            pageCursor = nodePage( reference );
        }
        this.next = reference >= 0 ? reference : NO_ID;
        //This marks the cursor as a "single cursor"
        this.highMark = NO_ID;
        this.nextStoreReference = NO_ID;
        this.open = true;
        this.batched = false;
    }

    @Override
    public boolean scanBatch( AllNodeScan scan, int sizeHint )
    {
        if ( getId() != NO_ID )
        {
            reset();
        }
        this.batched = true;
        this.open = true;
        this.nextStoreReference = NO_ID;

        return ((RecordNodeScan) scan).scanBatch( sizeHint , this);
    }

    boolean scanRange( long start, long stop )
    {
        long max = nodeHighMark();
        if ( start > max )
        {
            reset();
            return false;
        }
        if ( start > stop )
        {
            reset();
            return true;
        }
        if ( pageCursor == null )
        {
            pageCursor = nodePage( start );
        }
        next = start;
        highMark = min( stop, max );
        return true;
    }

    @Override
    public long entityReference()
    {
        return getId();
    }

    @Override
    public long[] labels()
    {
        return NodeLabelsField.get( this, read, cursorTracer );
    }

    @Override
    public boolean hasLabel( int label )
    {
        return NodeLabelsField.hasLabel( this, read, cursorTracer, label );
    }

    @Override
    public boolean hasProperties()
    {
        return nextProp != NO_ID;
    }

    @Override
    public long relationshipsReference()
    {
        return relationshipsReferenceWithDenseMarker( getNextRel(), isDense() );
    }

    /**
     * Marks a relationships reference with a special flag if the node is dense, because if that case the reference actually points
     * to a relationship group record.
     */
    static long relationshipsReferenceWithDenseMarker( long nextRel, boolean isDense )
    {
        return isDense ? encodeDense( nextRel ) : nextRel;
    }

    @Override
    public void relationships( StorageRelationshipTraversalCursor traversalCursor, RelationshipSelection selection )
    {
        ((RecordRelationshipTraversalCursor) traversalCursor).init( this, selection );
    }

    @Override
    public int[] relationshipTypes()
    {
        MutableIntSet types = IntSets.mutable.empty();
        if ( !isDense() )
        {
            ensureRelationshipTraversalCursorInitialized();
            relationshipCursor.init( this, ALL_RELATIONSHIPS );
            while ( relationshipCursor.next() )
            {
                types.add( relationshipCursor.type() );
            }
        }
        else
        {
            if ( groupCursor == null )
            {
                groupCursor = new RecordRelationshipGroupCursor( relationshipStore, groupStore, cursorTracer, loadMode );
            }
            groupCursor.init( entityReference(), getNextRel(), true );
            while ( groupCursor.next() )
            {
                types.add( groupCursor.getType() );
            }
        }
        return types.toArray();
    }

    private void ensureRelationshipTraversalCursorInitialized()
    {
        if ( relationshipCursor == null )
        {
            relationshipCursor = new RecordRelationshipTraversalCursor( relationshipStore, groupStore, cursorTracer );
        }
    }

    private void ensureRelationshipScanCursorInitialized()
    {
        if ( relationshipScanCursor == null )
        {
            relationshipScanCursor = new RecordRelationshipScanCursor( relationshipStore, cursorTracer );
        }
    }

    @Override
    public void degrees( RelationshipSelection selection, Degrees.Mutator mutator, boolean allowFastDegreeLookup )
    {
        if ( !mutator.isSplit() && !isDense() && allowFastDegreeLookup && !selection.isLimited() )
        {
            // There's an optimization for getting only the total degree directly and we're not limited by security
            ensureRelationshipScanCursorInitialized();
            relationshipScanCursor.single( getNextRel() );
            if ( relationshipScanCursor.next() )
            {
                int degree = relationshipScanCursor.sourceNodeReference() == getId()
                        ? (int) relationshipScanCursor.getFirstPrevRel()
                        : (int) relationshipScanCursor.getSecondPrevRel();
                mutator.add( -1, degree, 0, 0 );
            }
            return;
        }

        if ( !isDense() || !allowFastDegreeLookup )
        {
            ensureRelationshipTraversalCursorInitialized();
            relationshipCursor.init( this, ALL_RELATIONSHIPS );
            while ( relationshipCursor.next() )
            {
                if ( selection.test( relationshipCursor.type(), relationshipCursor.sourceNodeReference(), relationshipCursor.targetNodeReference() ) )
                {
                    int outgoing = 0;
                    int incoming = 0;
                    int loop = 0;
                    if ( relationshipCursor.sourceNodeReference() == entityReference() )
                    {
                        if ( relationshipCursor.targetNodeReference() == entityReference() )
                        {
                            loop++;
                        }
                        else if ( selection.test( RelationshipDirection.OUTGOING ) )
                        {
                            outgoing++;
                        }
                    }
                    else if ( selection.test( RelationshipDirection.INCOMING ) )
                    {
                        incoming++;
                    }
                    if ( !mutator.add( relationshipCursor.type(), outgoing, incoming, loop ) )
                    {
                        return;
                    }
                }
            }
        }
        else
        {
            if ( groupCursor == null )
            {
                groupCursor = new RecordRelationshipGroupCursor( relationshipStore, groupStore, cursorTracer, loadMode );
            }
            groupCursor.init( entityReference(), getNextRel(), isDense() );
            int criteriaMet = 0;
            boolean typeLimited = selection.isTypeLimited();
            int numCriteria = selection.numberOfCriteria();
            while ( groupCursor.next() )
            {
                if ( selection.test( groupCursor.getType() ) )
                {
                    int outgoing = 0;
                    int incoming = 0;
                    int loop = groupCursor.loopCount();
                    if ( selection.test( RelationshipDirection.OUTGOING ) )
                    {
                        outgoing = groupCursor.outgoingCount();
                    }
                    if ( selection.test( RelationshipDirection.INCOMING ) )
                    {
                        incoming = groupCursor.incomingCount();
                    }
                    if ( !mutator.add( groupCursor.getType(), outgoing, incoming, loop ) )
                    {
                        return;
                    }
                    if ( typeLimited && ++criteriaMet >= numCriteria )
                    {
                        break;
                    }
                }
            }
        }
    }

    @Override
    public boolean supportsFastDegreeLookup()
    {
        return isDense();
    }

    @Override
    public void setForceLoad()
    {
        this.loadMode = RecordLoadOverride.FORCE;
        if ( groupCursor != null )
        {
            groupCursor.loadMode = RecordLoadOverride.FORCE;
        }
    }

    @Override
    public long propertiesReference()
    {
        return getNextProp();
    }

    @Override
    public void properties( StoragePropertyCursor propertyCursor )
    {
        propertyCursor.initNodeProperties( getNextProp() );
    }

    @Override
    public boolean next()
    {
        if ( next == NO_ID )
        {
            resetState();
            return false;
        }

        do
        {
            if ( nextStoreReference == next )
            {
                nodeAdvance( this, pageCursor );
                next++;
                nextStoreReference++;
            }
            else
            {
                node( this, next++, pageCursor );
                nextStoreReference = next;
            }

            if ( next > highMark )
            {
                if ( isSingle() || batched )
                {
                    //we are a "single cursor" or a "batched scan"
                    //we don't want to set a new highMark
                    next = NO_ID;
                    return inUse();
                }
                else
                {
                    //we are a "scan cursor"
                    //Check if there is a new high mark
                    highMark = nodeHighMark();
                    if ( next > highMark )
                    {
                        next = NO_ID;
                        return inUse();
                    }
                }
            }
        }
        while ( !inUse() );
        return true;
    }

    @Override
    public void reset()
    {
        if ( open )
        {
            open = false;
            resetState();
        }
    }

    private void resetState()
    {
        next = NO_ID;
        setId( NO_ID );
        clear();
        this.loadMode = RecordLoadOverride.none();
        if ( groupCursor != null )
        {
            groupCursor.loadMode = RecordLoadOverride.none();
        }
    }

    private boolean isSingle()
    {
        return highMark == NO_ID;
    }

    @Override
    public RecordNodeCursor copy()
    {
        throw new UnsupportedOperationException( "Record cursors are not copyable." );
    }

    @Override
    public String toString()
    {
        if ( !open )
        {
            return "RecordNodeCursor[closed state]";
        }
        else
        {
            return "RecordNodeCursor[id=" + getId() +
                    ", open state with: highMark=" + highMark +
                    ", next=" + next +
                    ", underlying record=" + super.toString() + "]";
        }
    }

    @Override
    public void close()
    {
        if ( pageCursor != null )
        {
            pageCursor.close();
            pageCursor = null;
        }
        if ( groupCursor != null )
        {
            groupCursor.close();
            groupCursor = null;
        }
        if ( relationshipCursor != null )
        {
            relationshipCursor.close();
            relationshipCursor = null;
        }
        if ( relationshipScanCursor != null )
        {
            relationshipScanCursor.close();
            relationshipScanCursor = null;
        }
    }

    private PageCursor nodePage( long reference )
    {
        return read.openPageCursorForReading( reference, cursorTracer );
    }

    private long nodeHighMark()
    {
        return read.getHighestPossibleIdInUse( cursorTracer );
    }

    private void node( NodeRecord record, long reference, PageCursor pageCursor )
    {
        read.getRecordByCursor( reference, record, loadMode.orElse( RecordLoad.CHECK ).lenient(), pageCursor );
    }

    private void nodeAdvance( NodeRecord record, PageCursor pageCursor )
    {
        read.nextRecordByCursor( record, loadMode.orElse( RecordLoad.CHECK ).lenient(), pageCursor );
    }
}

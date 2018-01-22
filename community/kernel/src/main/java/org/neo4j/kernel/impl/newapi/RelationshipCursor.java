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
package org.neo4j.kernel.impl.newapi;

import java.util.Set;

import org.neo4j.internal.kernel.api.RelationshipDataAccessor;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static java.util.Collections.emptySet;

abstract class RelationshipCursor extends RelationshipRecord implements RelationshipDataAccessor, RelationshipVisitor<RuntimeException>
{
    Read read;
    private HasChanges hasChanges = HasChanges.MAYBE;
    Set<Long> addedRelationships;

    RelationshipCursor()
    {
        super( NO_ID );
    }

    protected void init( Read read )
    {
        this.read = read;
        this.hasChanges = HasChanges.MAYBE;
        this.addedRelationships = emptySet();
    }

    @Override
    public long relationshipReference()
    {
        return getId();
    }

    @Override
    public int label()
    {
        return getType();
    }

    @Override
    public boolean hasProperties()
    {
        return nextProp != PropertyCursor.NO_ID;
    }

    @Override
    public void source( org.neo4j.internal.kernel.api.NodeCursor cursor )
    {
        read.singleNode( sourceNodeReference(), cursor );
    }

    @Override
    public void target( org.neo4j.internal.kernel.api.NodeCursor cursor )
    {
        read.singleNode( targetNodeReference(), cursor );
    }

    @Override
    public void properties( org.neo4j.internal.kernel.api.PropertyCursor cursor )
    {
        read.relationshipProperties( relationshipReference(), propertiesReference(), cursor );
    }

    @Override
    public long sourceNodeReference()
    {
        return getFirstNode();
    }

    @Override
    public long targetNodeReference()
    {
        return getSecondNode();
    }

    @Override
    public long propertiesReference()
    {
        return getNextProp();
    }

    protected abstract boolean shouldGetAddedTxStateSnapshot();

    /**
     * RelationshipCursor should only see changes that are there from the beginning
     * otherwise it will not be stable.
     */
    protected boolean hasChanges()
    {
        switch ( hasChanges )
        {
        case MAYBE:
            boolean changes = read.hasTxStateWithChanges();
            if ( changes )
            {
                if ( shouldGetAddedTxStateSnapshot() )
                {
                    addedRelationships = read.txState().addedAndRemovedRelationships().getAddedSnapshot();
                }
                hasChanges = HasChanges.YES;
            }
            else
            {
                hasChanges = HasChanges.NO;
            }
            return changes;
        case YES:
            return true;
        case NO:
            return false;
        default:
            throw new IllegalStateException( "Style guide, why are you making me do this" );
        }
    }

    // Load transaction state using RelationshipVisitor
    protected void loadFromTxState( long reference )
    {
        read.txState().relationshipVisit( reference, this );
    }

    // used to visit transaction state
    @Override
    public void visit( long relationshipId, int typeId, long startNodeId, long endNodeId )
    {
        initialize( true, NO_ID, startNodeId, endNodeId, typeId, NO_ID, NO_ID, NO_ID, NO_ID, false, false );
    }
}

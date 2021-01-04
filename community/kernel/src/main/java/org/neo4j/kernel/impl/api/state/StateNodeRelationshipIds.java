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
package org.neo4j.kernel.impl.api.state;

import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.txstate.RelationshipModifications;

import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.storageengine.api.txstate.RelationshipModifications.noAdditionalDataDecorator;

class StateNodeRelationshipIds implements RelationshipModifications.NodeRelationshipIds
{
    private static final long SHALLOW_SIZE = shallowSizeOfInstance( StateNodeRelationshipIds.class );
    private final NodeStateImpl nodeState;
    private final boolean hasCreations;
    private final boolean hasDeletions;
    private final RelationshipModifications.IdDataDecorator relationshipVisit;

    static StateNodeRelationshipIds createStateNodeRelationshipIds( NodeStateImpl nodeState, RelationshipModifications.IdDataDecorator relationshipVisit,
            MemoryTracker memoryTracker )
    {
        memoryTracker.allocateHeap( SHALLOW_SIZE );
        return new StateNodeRelationshipIds( nodeState, relationshipVisit );
    }

    private StateNodeRelationshipIds( NodeStateImpl nodeState, RelationshipModifications.IdDataDecorator relationshipVisit )
    {
        this.nodeState = nodeState;
        this.hasCreations = nodeState.hasAddedRelationships();
        this.hasDeletions = nodeState.hasRemovedRelationships();
        this.relationshipVisit = relationshipVisit;
    }

    @Override
    public long nodeId()
    {
        return nodeState.getId();
    }

    @Override
    public boolean hasCreations()
    {
        return hasCreations;
    }

    @Override
    public boolean hasCreations( int type )
    {
        return hasCreations && nodeState.hasAddedRelationships( type );
    }

    @Override
    public boolean hasDeletions()
    {
        return hasDeletions;
    }

    @Override
    public RelationshipModifications.RelationshipBatch creations()
    {
        return nodeState.additionsAsRelationshipBatch( relationshipVisit );
    }

    @Override
    public RelationshipModifications.RelationshipBatch deletions()
    {
        return nodeState.removalsAsRelationshipBatch( noAdditionalDataDecorator() );
    }

    @Override
    public void forEachCreationSplitInterruptible(
            RelationshipModifications.InterruptibleTypeIdsVisitor visitor )
    {
        nodeState.visitAddedIdsSplit( visitor, relationshipVisit );
    }

    @Override
    public void forEachDeletionSplitInterruptible(
            RelationshipModifications.InterruptibleTypeIdsVisitor visitor )
    {
        nodeState.visitRemovedIdsSplit( visitor );
    }
}

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
package org.neo4j.kernel.impl.transaction.state;

import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.state.RecordAccess.RecordProxy;
import org.neo4j.kernel.impl.util.DirectionWrapper;
import org.neo4j.storageengine.api.lock.ResourceLocker;

public class RelationshipCreator
{
    private final RelationshipGroupGetter relGroupGetter;
    private final int denseNodeThreshold;

    public RelationshipCreator( RelationshipGroupGetter relGroupGetter, int denseNodeThreshold )
    {
        this.relGroupGetter = relGroupGetter;
        this.denseNodeThreshold = denseNodeThreshold;
    }

    /**
     * Creates a relationship with the given id, from the nodes identified by id
     * and of type typeId
     *
     * @param id The id of the relationship to create.
     * @param type The id of the relationship type this relationship will
     *            have.
     * @param firstNodeId The id of the start node.
     * @param secondNodeId The id of the end node.
     */
    public void relationshipCreate( long id, int type, long firstNodeId, long secondNodeId,
            RecordAccessSet recordChangeSet, ResourceLocker locks )
    {
        // TODO could be unnecessary to mark as changed here already, dense nodes may not need to change
        NodeRecord firstNode = recordChangeSet.getNodeRecords().getOrLoad( firstNodeId, null ).forChangingLinkage();
        NodeRecord secondNode = recordChangeSet.getNodeRecords().getOrLoad( secondNodeId, null ).forChangingLinkage();
        convertNodeToDenseIfNecessary( firstNode, recordChangeSet.getRelRecords(),
                recordChangeSet.getRelGroupRecords(), locks );
        convertNodeToDenseIfNecessary( secondNode, recordChangeSet.getRelRecords(),
                recordChangeSet.getRelGroupRecords(), locks );
        RelationshipRecord record = recordChangeSet.getRelRecords().create( id, null ).forChangingLinkage();
        record.setLinks( firstNodeId, secondNodeId, type );
        record.setInUse( true );
        record.setCreated();
        connectRelationship( firstNode, secondNode, record, recordChangeSet.getRelRecords(),
                recordChangeSet.getRelGroupRecords(), locks );
    }

    public static int relCount( long nodeId, RelationshipRecord rel )
    {
        return (int) (nodeId == rel.getFirstNode() ? rel.getFirstPrevRel() : rel.getSecondPrevRel());
    }

    private void convertNodeToDenseIfNecessary( NodeRecord node,
            RecordAccess<RelationshipRecord, Void> relRecords,
            RecordAccess<RelationshipGroupRecord, Integer> relGroupRecords, ResourceLocker locks )
    {
        if ( node.isDense() )
        {
            return;
        }
        long relId = node.getNextRel();
        if ( relId != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            RecordProxy<RelationshipRecord, Void> relChange = relRecords.getOrLoad( relId, null );
            RelationshipRecord rel = relChange.forReadingLinkage();
            if ( relCount( node.getId(), rel ) >= denseNodeThreshold )
            {
                locks.acquireExclusive( LockTracer.NONE, ResourceTypes.RELATIONSHIP, relId );
                // Re-read the record after we've locked it since another transaction might have
                // changed in the meantime.
                relChange = relRecords.getOrLoad( relId, null );

                convertNodeToDenseNode( node, relChange.forChangingLinkage(), relRecords, relGroupRecords, locks );
            }
        }
    }

    private void connectRelationship( NodeRecord firstNode,
            NodeRecord secondNode, RelationshipRecord rel,
            RecordAccess<RelationshipRecord, Void> relRecords,
            RecordAccess<RelationshipGroupRecord, Integer> relGroupRecords, ResourceLocker locks )
    {
        // Assertion interpreted: if node is a normal node and we're trying to create a
        // relationship that we already have as first rel for that node --> error
        assert firstNode.getNextRel() != rel.getId() || firstNode.isDense();
        assert secondNode.getNextRel() != rel.getId() || secondNode.isDense();

        if ( !firstNode.isDense() )
        {
            rel.setFirstNextRel( firstNode.getNextRel() );
        }
        if ( !secondNode.isDense() )
        {
            rel.setSecondNextRel( secondNode.getNextRel() );
        }

        if ( !firstNode.isDense() )
        {
            connect( firstNode, rel, relRecords, locks );
        }
        else
        {
            connectRelationshipToDenseNode( firstNode, rel, relRecords, relGroupRecords, locks );
        }

        if ( !secondNode.isDense() )
        {
            if ( firstNode.getId() != secondNode.getId() )
            {
                connect( secondNode, rel, relRecords, locks );
            }
            else
            {
                rel.setFirstInFirstChain( true );
                rel.setSecondPrevRel( rel.getFirstPrevRel() );
            }
        }
        else if ( firstNode.getId() != secondNode.getId() )
        {
            connectRelationshipToDenseNode( secondNode, rel, relRecords, relGroupRecords, locks );
        }

        if ( !firstNode.isDense() )
        {
            firstNode.setNextRel( rel.getId() );
        }
        if ( !secondNode.isDense() )
        {
            secondNode.setNextRel( rel.getId() );
        }
    }

    private void connectRelationshipToDenseNode( NodeRecord node, RelationshipRecord rel,
            RecordAccess<RelationshipRecord, Void> relRecords,
            RecordAccess<RelationshipGroupRecord, Integer> relGroupRecords, ResourceLocker locks )
    {
        RelationshipGroupRecord group =
                relGroupGetter.getOrCreateRelationshipGroup( node, rel.getType(), relGroupRecords ).forChangingData();
        DirectionWrapper dir = DirectionIdentifier.wrapDirection( rel, node );
        long nextRel = dir.getNextRel( group );
        setCorrectNextRel( node, rel, nextRel );
        connect( node.getId(), nextRel, rel, relRecords, locks );
        dir.setNextRel( group, rel.getId() );
    }

    private void connect( NodeRecord node, RelationshipRecord rel,
            RecordAccess<RelationshipRecord, Void> relRecords, ResourceLocker locks )
    {
        connect( node.getId(), node.getNextRel(), rel, relRecords, locks );
    }

    private void convertNodeToDenseNode( NodeRecord node, RelationshipRecord firstRel,
            RecordAccess<RelationshipRecord, Void> relRecords,
            RecordAccess<RelationshipGroupRecord, Integer> relGroupRecords, ResourceLocker locks )
    {
        node.setDense( true );
        node.setNextRel( Record.NO_NEXT_RELATIONSHIP.intValue() );
        long relId = firstRel.getId();
        RelationshipRecord relRecord = firstRel;
        while ( relId != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            // Get the next relationship id before connecting it (where linkage is overwritten)
            relId = relChain( relRecord, node.getId() ).get( relRecord );
            connectRelationshipToDenseNode( node, relRecord, relRecords, relGroupRecords, locks );
            if ( relId != Record.NO_NEXT_RELATIONSHIP.intValue() )
            {   // Lock and load the next relationship in the chain
                locks.acquireExclusive( LockTracer.NONE, ResourceTypes.RELATIONSHIP, relId );
                relRecord = relRecords.getOrLoad( relId, null ).forChangingLinkage();
            }
        }
    }

    private void connect( long nodeId, long firstRelId, RelationshipRecord rel,
            RecordAccess<RelationshipRecord, Void> relRecords, ResourceLocker locks )
    {
        long newCount = 1;
        if ( firstRelId != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            locks.acquireExclusive( LockTracer.NONE, ResourceTypes.RELATIONSHIP, firstRelId );
            RelationshipRecord firstRel = relRecords.getOrLoad( firstRelId, null ).forChangingLinkage();
            boolean changed = false;
            if ( firstRel.getFirstNode() == nodeId )
            {
                newCount = firstRel.getFirstPrevRel() + 1;
                firstRel.setFirstPrevRel( rel.getId() );
                firstRel.setFirstInFirstChain( false );
                changed = true;
            }
            if ( firstRel.getSecondNode() == nodeId )
            {
                newCount = firstRel.getSecondPrevRel() + 1;
                firstRel.setSecondPrevRel( rel.getId() );
                firstRel.setFirstInSecondChain( false );
                changed = true;
            }
            if ( !changed )
            {
                throw new InvalidRecordException( nodeId + " doesn't match " + firstRel );
            }
        }

        // Set the relationship count
        if ( rel.getFirstNode() == nodeId )
        {
            rel.setFirstPrevRel( newCount );
            rel.setFirstInFirstChain( true );
        }
        if ( rel.getSecondNode() == nodeId )
        {
            rel.setSecondPrevRel( newCount );
            rel.setFirstInSecondChain( true );
        }
    }

    private void setCorrectNextRel( NodeRecord node, RelationshipRecord rel, long nextRel )
    {
        if ( node.getId() == rel.getFirstNode() )
        {
            rel.setFirstNextRel( nextRel );
        }
        if ( node.getId() == rel.getSecondNode() )
        {
            rel.setSecondNextRel( nextRel );
        }
    }

    private static RelationshipConnection relChain( RelationshipRecord rel, long nodeId )
    {
        if ( rel.getFirstNode() == nodeId )
        {
            return RelationshipConnection.START_NEXT;
        }
        if ( rel.getSecondNode() == nodeId )
        {
            return RelationshipConnection.END_NEXT;
        }
        throw new RuntimeException( nodeId + " neither start not end node in " + rel );
    }
}

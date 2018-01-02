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
package org.neo4j.kernel.impl.transaction.state;

import static org.neo4j.kernel.impl.transaction.state.RelationshipCreator.relCount;

import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.state.RecordAccess.RecordProxy;

import org.neo4j.kernel.impl.util.DirectionWrapper;

public class RelationshipDeleter
{
    private final Locks.Client locks;
    private final RelationshipGroupGetter relGroupGetter;
    private final PropertyDeleter propertyChainDeleter;

    public RelationshipDeleter( Locks.Client locks, RelationshipGroupGetter relGroupGetter,
                                PropertyDeleter propertyChainDeleter )
    {
        this.locks = locks;
        this.relGroupGetter = relGroupGetter;
        this.propertyChainDeleter = propertyChainDeleter;
    }

    /**
     * Deletes a relationship by its id, returning its properties which are now
     * removed. It is assumed that the nodes it connects have already been
     * deleted in this
     * transaction.
     *
     * @param id The id of the relationship to delete.
     * @return The properties of the relationship that were removed during the
     *         delete.
     */
    public void relDelete( long id, RecordAccessSet recordChanges )
    {
        RelationshipRecord record = recordChanges.getRelRecords().getOrLoad( id, null ).forChangingLinkage();
        propertyChainDeleter.deletePropertyChain( record, recordChanges.getPropertyRecords() );
        disconnectRelationship( record, recordChanges );
        updateNodesForDeletedRelationship( record, recordChanges );
        record.setInUse( false );
    }

    private void disconnectRelationship( RelationshipRecord rel, RecordAccessSet recordChangeSet )
    {
        disconnect( rel, RelationshipConnection.START_NEXT, recordChangeSet.getRelRecords() );
        disconnect( rel, RelationshipConnection.START_PREV, recordChangeSet.getRelRecords() );
        disconnect( rel, RelationshipConnection.END_NEXT, recordChangeSet.getRelRecords() );
        disconnect( rel, RelationshipConnection.END_PREV, recordChangeSet.getRelRecords() );
    }

    private void disconnect( RelationshipRecord rel, RelationshipConnection pointer,
                             RecordAccess<Long, RelationshipRecord, Void> relChanges )
    {
        long otherRelId = pointer.otherSide().get( rel );
        if ( otherRelId == Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            return;
        }

        locks.acquireExclusive( ResourceTypes.RELATIONSHIP, otherRelId );
        RelationshipRecord otherRel = relChanges.getOrLoad( otherRelId, null ).forChangingLinkage();
        boolean changed = false;
        long newId = pointer.get( rel );
        boolean newIsFirst = pointer.isFirstInChain( rel );
        if ( otherRel.getFirstNode() == pointer.compareNode( rel ) )
        {
            pointer.start().set( otherRel, newId, newIsFirst );
            changed = true;
        }
        if ( otherRel.getSecondNode() == pointer.compareNode( rel ) )
        {
            pointer.end().set( otherRel, newId, newIsFirst );
            changed = true;
        }
        if ( !changed )
        {
            throw new InvalidRecordException( otherRel + " don't match " + rel );
        }
    }

    private void updateNodesForDeletedRelationship( RelationshipRecord rel, RecordAccessSet recordChanges )
    {
        RecordProxy<Long, NodeRecord, Void> startNodeChange =
                recordChanges.getNodeRecords().getOrLoad( rel.getFirstNode(), null );
        RecordProxy<Long, NodeRecord, Void> endNodeChange =
                recordChanges.getNodeRecords().getOrLoad( rel.getSecondNode(), null );

        NodeRecord startNode = recordChanges.getNodeRecords().getOrLoad( rel.getFirstNode(), null ).forReadingLinkage();
        NodeRecord endNode = recordChanges.getNodeRecords().getOrLoad( rel.getSecondNode(), null ).forReadingLinkage();
        boolean loop = startNode.getId() == endNode.getId();

        if ( !startNode.isDense() )
        {
            if ( rel.isFirstInFirstChain() )
            {
                startNode = startNodeChange.forChangingLinkage();
                startNode.setNextRel( rel.getFirstNextRel() );
            }
            decrementTotalRelationshipCount( startNode.getId(), rel, startNode.getNextRel(),
                    recordChanges.getRelRecords() );
        }
        else
        {
            RecordProxy<Long, RelationshipGroupRecord, Integer> groupChange =
                    relGroupGetter.getRelationshipGroup( startNode, rel.getType(),
                            recordChanges.getRelGroupRecords() ).group();
            assert groupChange != null : "Relationship group " + rel.getType() + " should have existed here";
            RelationshipGroupRecord group = groupChange.forReadingData();
            DirectionWrapper dir = DirectionIdentifier.wrapDirection( rel, startNode );
            if ( rel.isFirstInFirstChain() )
            {
                group = groupChange.forChangingData();
                dir.setNextRel( group, rel.getFirstNextRel() );
                if ( groupIsEmpty( group ) )
                {
                    deleteGroup( startNodeChange, group, recordChanges.getRelGroupRecords() );
                }
            }
            decrementTotalRelationshipCount( startNode.getId(), rel, dir.getNextRel( group ),
                    recordChanges.getRelRecords() );
        }

        if ( !endNode.isDense() )
        {
            if ( rel.isFirstInSecondChain() )
            {
                endNode = endNodeChange.forChangingLinkage();
                endNode.setNextRel( rel.getSecondNextRel() );
            }
            if ( !loop )
            {
                decrementTotalRelationshipCount( endNode.getId(), rel, endNode.getNextRel(),
                        recordChanges.getRelRecords() );
            }
        }
        else
        {
            RecordProxy<Long, RelationshipGroupRecord, Integer> groupChange =
                    relGroupGetter.getRelationshipGroup( endNode, rel.getType(),
                            recordChanges.getRelGroupRecords() ).group();
            DirectionWrapper dir = DirectionIdentifier.wrapDirection( rel, endNode );
            assert groupChange != null || loop : "Group has been deleted";
            if ( groupChange != null )
            {
                RelationshipGroupRecord group = groupChange.forReadingData();
                if ( rel.isFirstInSecondChain() )
                {
                    group = groupChange.forChangingData();
                    dir.setNextRel( group, rel.getSecondNextRel() );
                    if ( groupIsEmpty( group ) )
                    {
                        deleteGroup( endNodeChange, group, recordChanges.getRelGroupRecords() );
                    }
                }
            } // Else this is a loop-rel and the group was deleted when dealing with the start node
            if ( !loop )
            {
                decrementTotalRelationshipCount( endNode.getId(), rel, dir.getNextRel( groupChange.forChangingData() ),
                        recordChanges.getRelRecords() );
            }
        }
    }

    private boolean decrementTotalRelationshipCount( long nodeId, RelationshipRecord rel, long firstRelId,
                                                     RecordAccess<Long, RelationshipRecord, Void> relRecords )
    {
        if ( firstRelId == Record.NO_PREV_RELATIONSHIP.intValue() )
        {
            return true;
        }
        boolean firstInChain = relIsFirstInChain( nodeId, rel );
        if ( !firstInChain )
        {
            locks.acquireExclusive( ResourceTypes.RELATIONSHIP, firstRelId );
        }
        RelationshipRecord firstRel = relRecords.getOrLoad( firstRelId, null ).forChangingLinkage();
        if ( nodeId == firstRel.getFirstNode() )
        {
            firstRel.setFirstPrevRel( firstInChain ?
                    relCount( nodeId, rel )-1 : relCount( nodeId, firstRel ) - 1 );
            firstRel.setFirstInFirstChain( true );
        }
        if ( nodeId == firstRel.getSecondNode() )
        {
            firstRel.setSecondPrevRel( firstInChain ?
                    relCount( nodeId, rel )-1 :
                    relCount( nodeId, firstRel )-1 );
            firstRel.setFirstInSecondChain( true );
        }
        return false;
    }

    private void deleteGroup( RecordProxy<Long, NodeRecord, Void> nodeChange,
                              RelationshipGroupRecord group,
                              RecordAccess<Long, RelationshipGroupRecord, Integer> relGroupRecords )
    {
        long previous = group.getPrev();
        long next = group.getNext();
        if ( previous == Record.NO_NEXT_RELATIONSHIP.intValue() )
        {   // This is the first one, just point the node to the next group
            nodeChange.forChangingLinkage().setNextRel( next );
        }
        else
        {   // There are others before it, point the previous to the next group
            RelationshipGroupRecord previousRecord = relGroupRecords.getOrLoad( previous, null ).forChangingLinkage();
            previousRecord.setNext( next );
        }

        if ( next != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {   // There are groups after this one, point that next group to the previous of the group to be deleted
            RelationshipGroupRecord nextRecord = relGroupRecords.getOrLoad( next, null ).forChangingLinkage();
            nextRecord.setPrev( previous );
        }
        group.setInUse( false );
    }

    private boolean groupIsEmpty( RelationshipGroupRecord group )
    {
        return group.getFirstOut() == Record.NO_NEXT_RELATIONSHIP.intValue() &&
                group.getFirstIn() == Record.NO_NEXT_RELATIONSHIP.intValue() &&
                group.getFirstLoop() == Record.NO_NEXT_RELATIONSHIP.intValue();
    }

    private boolean relIsFirstInChain( long nodeId, RelationshipRecord rel )
    {
        return (nodeId == rel.getFirstNode() && rel.isFirstInFirstChain()) ||
                (nodeId == rel.getSecondNode() && rel.isFirstInSecondChain());
    }
}

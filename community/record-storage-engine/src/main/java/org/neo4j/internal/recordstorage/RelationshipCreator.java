/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.internal.counts.RelationshipGroupDegreesStore;
import org.neo4j.internal.recordstorage.RecordAccess.RecordProxy;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.txstate.RelationshipModifications;
import org.neo4j.storageengine.api.txstate.RelationshipModifications.RelationshipBatch;

import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;
import static org.neo4j.kernel.impl.store.record.Record.isNull;

public class RelationshipCreator
{
    private final int denseNodeThreshold;
    private final long externalDegreesThreshold;
    private final PageCursorTracer cursorTracer;

    public RelationshipCreator( int denseNodeThreshold, long externalDegreesThreshold, PageCursorTracer cursorTracer )
    {
        this.denseNodeThreshold = denseNodeThreshold;
        this.externalDegreesThreshold = externalDegreesThreshold;
        this.cursorTracer = cursorTracer;
    }

    public interface NodeDataLookup
    {
        int DIR_OUT = 0;
        int DIR_IN = 1;
        int DIR_LOOP = 2;

        RecordProxy<RelationshipRecord,Void> entryPoint( long nodeId, int type, int direction );

        RecordProxy<RelationshipGroupRecord,Integer> group( long nodeId, int type, boolean create );
    }

    public static NodeDataLookup insertFirst( RelationshipGroupGetter relationshipGroupGetter, RecordAccessSet recordChanges, PageCursorTracer cursorTracer )
    {
        return new RelationshipCreator.NodeDataLookup()
        {
            @Override
            public RecordProxy<RelationshipRecord,Void> entryPoint( long nodeId, int type, int direction )
            {
                return null;
            }

            @Override
            public RecordProxy<RelationshipGroupRecord,Integer> group( long nodeId, int type, boolean create )
            {
                RecordProxy<NodeRecord,Void> nodeChange = recordChanges.getNodeRecords().getOrLoad( nodeId, null, cursorTracer );
                return relationshipGroupGetter.getOrCreateRelationshipGroup( nodeChange, type, recordChanges.getRelGroupRecords() );
            }
        };
    }

    /**
     * Creates all relationships in {@code creations}
     * @param creations {@link RelationshipModifications} with all relationship creations in this transaction.
     * @param recordChanges all record changes.
     * @param groupDegreesUpdater updater of external group degrees for dense nodes.
     * @param nodeDataLookup for looking up where to insert relationships.
     */
    public void relationshipCreate( RelationshipBatch creations, RecordAccessSet recordChanges, RelationshipGroupDegreesStore.Updater groupDegreesUpdater,
            NodeDataLookup nodeDataLookup )
    {
        creations.forEach( ( id, type, firstNodeId, secondNodeId ) ->
                relationshipCreate( id, type, firstNodeId, secondNodeId, recordChanges, groupDegreesUpdater, nodeDataLookup ) );
    }

    /**
     * Creates a relationship with the given id, from the nodes identified by id and of type typeId
     *
     * @param id The id of the relationship to create.
     * @param type The id of the relationship type this relationship will have.
     * @param firstNodeId The id of the start node.
     * @param secondNodeId The id of the end node.
     */
    public void relationshipCreate( long id, int type, long firstNodeId, long secondNodeId, RecordAccessSet recordChanges,
            RelationshipGroupDegreesStore.Updater groupDegreesUpdater, NodeDataLookup nodeDataLookup )
    {
        RecordProxy<NodeRecord,Void> firstNode = recordChanges.getNodeRecords().getOrLoad( firstNodeId, null, cursorTracer );
        RecordProxy<NodeRecord,Void> secondNode =
                firstNodeId == secondNodeId ? firstNode : recordChanges.getNodeRecords().getOrLoad( secondNodeId, null, cursorTracer );
        RecordAccess<RelationshipRecord,Void> relRecords = recordChanges.getRelRecords();
        convertNodeToDenseIfNecessary( firstNode, relRecords, groupDegreesUpdater, nodeDataLookup );
        convertNodeToDenseIfNecessary( secondNode, relRecords, groupDegreesUpdater, nodeDataLookup );
        RelationshipRecord record = relRecords.create( id, null, cursorTracer ).forChangingLinkage();
        record.setLinks( firstNodeId, secondNodeId, type );
        record.setInUse( true );
        record.setCreated();
        connectRelationship( firstNode, secondNode, record, relRecords, groupDegreesUpdater, nodeDataLookup );
    }

    static int relCount( long nodeId, RelationshipRecord rel )
    {
        return (int) rel.getPrevRel( nodeId );
    }

    private void convertNodeToDenseIfNecessary( RecordProxy<NodeRecord,Void> nodeChange, RecordAccess<RelationshipRecord,Void> relRecords,
            RelationshipGroupDegreesStore.Updater groupDegreesUpdater, NodeDataLookup nodeDataLookup )
    {
        NodeRecord node = nodeChange.forReadingLinkage();
        if ( node.isDense() )
        {
            return;
        }
        long relId = node.getNextRel();
        if ( !isNull( relId ) )
        {
            RecordProxy<RelationshipRecord, Void> relProxy = relRecords.getOrLoad( relId, null, cursorTracer );
            if ( relCount( node.getId(), relProxy.forReadingData() ) >= denseNodeThreshold )
            {
                convertNodeToDenseNode( nodeChange, relProxy.forChangingLinkage(), relRecords, groupDegreesUpdater, nodeDataLookup );
            }
        }
    }

    private void connectRelationship( RecordProxy<NodeRecord,Void> firstNodeChange, RecordProxy<NodeRecord,Void> secondNodeChange, RelationshipRecord rel,
            RecordAccess<RelationshipRecord,Void> relRecords, RelationshipGroupDegreesStore.Updater groupDegreesUpdater, NodeDataLookup nodeDataLookup )
    {
        // Assertion interpreted: if node is a normal node and we're trying to create a
        // relationship that we already have as first rel for that node --> error
        NodeRecord firstNode = firstNodeChange.forReadingLinkage();
        NodeRecord secondNode = secondNodeChange.forReadingLinkage();
        assert firstNode.getNextRel() != rel.getId() || firstNode.isDense();
        assert secondNode.getNextRel() != rel.getId() || secondNode.isDense();

        boolean loop = firstNode.getId() == secondNode.getId();

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
            connectSparse( firstNode.getId(), firstNode.getNextRel(), rel, relRecords );
        }
        else
        {
            int index = loop ? NodeDataLookup.DIR_LOOP : NodeDataLookup.DIR_OUT;
            connectRelationshipToDenseNode( firstNodeChange, rel, relRecords, groupDegreesUpdater,
                    nodeDataLookup.entryPoint( firstNode.getId(), rel.getType(), index ), nodeDataLookup );
        }

        if ( !secondNode.isDense() )
        {
            if ( !loop )
            {
                connectSparse( secondNode.getId(), secondNode.getNextRel(), rel, relRecords );
            }
            else
            {
                rel.setFirstInFirstChain( true );
                rel.setSecondPrevRel( rel.getFirstPrevRel() );
            }
        }
        else if ( !loop )
        {
            connectRelationshipToDenseNode( secondNodeChange, rel, relRecords, groupDegreesUpdater,
                    nodeDataLookup.entryPoint( secondNode.getId(), rel.getType(), NodeDataLookup.DIR_IN ), nodeDataLookup );
        }

        if ( !firstNode.isDense() )
        {
            firstNodeChange.forChangingLinkage();
            firstNode.setNextRel( rel.getId() );
        }
        if ( !secondNode.isDense() )
        {
            secondNodeChange.forChangingLinkage();
            secondNode.setNextRel( rel.getId() );
        }
    }

    private void connectRelationshipToDenseNode( RecordProxy<NodeRecord,Void> nodeChange, RelationshipRecord createdRelationship,
            RecordAccess<RelationshipRecord,Void> relRecords, RelationshipGroupDegreesStore.Updater groupDegreesUpdater,
            RecordProxy<RelationshipRecord,Void> entrypoint, NodeDataLookup nodeDataLookup )
    {
        NodeRecord node = nodeChange.forReadingLinkage();
        DirectionWrapper dir = DirectionWrapper.wrapDirection( createdRelationship, node );
        connectDense( node, nodeDataLookup.group( nodeChange.getKey(), createdRelationship.getType(), true ), dir, createdRelationship, relRecords,
                groupDegreesUpdater, entrypoint );
    }

    private void convertNodeToDenseNode( RecordProxy<NodeRecord,Void> nodeChange, RelationshipRecord firstRel, RecordAccess<RelationshipRecord,Void> relRecords,
            RelationshipGroupDegreesStore.Updater groupDegreesUpdater, NodeDataLookup nodeDataLookup )
    {
        NodeRecord node = nodeChange.forChangingLinkage();
        node.setDense( true );
        node.setNextRel( NO_NEXT_RELATIONSHIP.intValue() );
        long relId = firstRel.getId();
        RelationshipRecord relRecord = firstRel;
        while ( !isNull( relId ) )
        {
            // Get the next relationship id before connecting it (where linkage is overwritten)
            relId = relRecord.getNextRel( node.getId() );
            relRecord.setPrevRel( NO_NEXT_RELATIONSHIP.longValue(), node.getId() );
            relRecord.setNextRel( NO_NEXT_RELATIONSHIP.longValue(), node.getId() );
            connectRelationshipToDenseNode( nodeChange, relRecord, relRecords, groupDegreesUpdater, null, nodeDataLookup );
            if ( !isNull( relId ) )
            {   // Lock and load the next relationship in the chain
                relRecord = relRecords.getOrLoad( relId, null, cursorTracer ).forChangingLinkage();
            }
        }
    }

    private void connectDense( NodeRecord node, RecordProxy<RelationshipGroupRecord,Integer> groupProxy, DirectionWrapper direction,
            RelationshipRecord createdRelationship, RecordAccess<RelationshipRecord,Void> relRecords, RelationshipGroupDegreesStore.Updater groupDegreesUpdater,
            RecordProxy<RelationshipRecord,Void> entrypoint )
    {
        long nodeId = node.getId();
        RelationshipGroupRecord group = groupProxy.forReadingLinkage();
        long firstRelId = direction.getNextRel( group );
        RecordProxy<RelationshipRecord,Void> rBefore = null;
        RecordProxy<RelationshipRecord,Void> rAfter = null;
        if ( entrypoint != null )
        {
            rBefore = entrypoint;
            long next = entrypoint.forReadingLinkage().getNextRel( nodeId );
            if ( !isNull( next ) )
            {
                rAfter = relRecords.getOrLoad( next, null, cursorTracer );
            }
        }

        // Here everything is known and locked, do the insertion
        if ( rBefore == null )
        {
            // first, i.e. there's no relationship at all for this type and direction or we failed to get a lock in the chain
            if ( !isNull( firstRelId ) )
            {
                RelationshipRecord firstRel = relRecords.getOrLoad( firstRelId, null, cursorTracer ).forChangingLinkage();
                assert firstRel.isFirstInChain( nodeId );
                firstRel.setFirstInChain( false, nodeId );
                createdRelationship.setNextRel( firstRelId, nodeId );
                createdRelationship.setPrevRel( firstRel.getPrevRel( nodeId ), nodeId );
                firstRel.setPrevRel( createdRelationship.getId(), nodeId );
            }
            else
            {
                createdRelationship.setPrevRel( 0, nodeId );
            }

            group = groupProxy.forChangingData();
            direction.setNextRel( group, createdRelationship.getId() );
            createdRelationship.setFirstInChain( true, nodeId );
            firstRelId = createdRelationship.getId();
        }
        else if ( rAfter != null )
        {
            // between
            createdRelationship.setFirstInChain( false, nodeId );
            // Link before <-> created
            RelationshipRecord before = rBefore.forChangingLinkage();
            before.setNextRel( createdRelationship.getId(), nodeId );
            createdRelationship.setPrevRel( before.getId(), nodeId );

            // Link created <-> after
            RelationshipRecord after = rAfter.forChangingLinkage();
            createdRelationship.setNextRel( after.getId(), nodeId );
            after.setPrevRel( createdRelationship.getId(), nodeId );
        }
        else
        {
            // last
            createdRelationship.setFirstInChain( false, nodeId );
            RelationshipRecord lastRelationship = rBefore.forChangingLinkage();
            lastRelationship.setNextRel( createdRelationship.getId(), nodeId );
            createdRelationship.setPrevRel( lastRelationship.getId(), nodeId );
        }

        //Degrees

        if ( direction.hasExternalDegrees( group ) ) //Optimistic reading is fine, as this is a one-way switch
        {
            groupDegreesUpdater.increment( group.getId(), direction.direction(), 1 );
        }
        else
        {
            RecordProxy<RelationshipRecord,Void> firstRelProxy = relRecords.getOrLoad( firstRelId, null, cursorTracer );
            long prevCount = firstRelProxy.forReadingLinkage().getPrevRel( nodeId );
            long count = prevCount + 1;

            if ( count > externalDegreesThreshold )
            {
                group = groupProxy.forChangingData();
                direction.setHasExternalDegrees( group );
                groupDegreesUpdater.increment( group.getId(), direction.direction(), count );
            }
            else
            {
                firstRelProxy.forChangingLinkage().setPrevRel( count, nodeId );
            }
        }
    }

    private void connectSparse( long nodeId, long firstRelId, RelationshipRecord createdRelationship, RecordAccess<RelationshipRecord,Void> relRecords )
    {
        long newCount = 1;
        if ( !isNull( firstRelId ) )
        {
            RelationshipRecord firstRel = relRecords.getOrLoad( firstRelId, null, cursorTracer ).forChangingLinkage();
            boolean changed = false;
            if ( firstRel.getFirstNode() == nodeId )
            {
                newCount = firstRel.getFirstPrevRel() + 1;
                firstRel.setFirstPrevRel( createdRelationship.getId() );
                firstRel.setFirstInFirstChain( false );
                changed = true;
            }
            if ( firstRel.getSecondNode() == nodeId )
            {
                newCount = firstRel.getSecondPrevRel() + 1;
                firstRel.setSecondPrevRel( createdRelationship.getId() );
                firstRel.setFirstInSecondChain( false );
                changed = true;
            }
            if ( !changed )
            {
                throw new InvalidRecordException( nodeId + " doesn't match " + firstRel );
            }
        }

        // Set the relationship count
        if ( createdRelationship.getFirstNode() == nodeId )
        {
            createdRelationship.setFirstPrevRel( newCount );
            createdRelationship.setFirstInFirstChain( true );
        }
        if ( createdRelationship.getSecondNode() == nodeId )
        {
            createdRelationship.setSecondPrevRel( newCount );
            createdRelationship.setFirstInSecondChain( true );
        }
    }
}

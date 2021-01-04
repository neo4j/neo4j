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

import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;

import java.util.Arrays;
import java.util.function.Predicate;

import org.neo4j.collection.trackable.HeapTrackingLongObjectHashMap;
import org.neo4j.internal.counts.RelationshipGroupDegreesStore;
import org.neo4j.internal.recordstorage.RecordAccess.RecordProxy;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.txstate.RelationshipModifications;
import org.neo4j.storageengine.api.txstate.RelationshipModifications.RelationshipBatch;

import static org.neo4j.collection.trackable.HeapTrackingCollections.newLongObjectMap;
import static org.neo4j.internal.recordstorage.RelationshipCreator.NodeDataLookup.DIR_IN;
import static org.neo4j.internal.recordstorage.RelationshipCreator.NodeDataLookup.DIR_LOOP;
import static org.neo4j.internal.recordstorage.RelationshipCreator.NodeDataLookup.DIR_OUT;
import static org.neo4j.internal.recordstorage.RelationshipCreator.relCount;
import static org.neo4j.internal.recordstorage.RelationshipLockHelper.findAndLockEntrypoint;
import static org.neo4j.internal.recordstorage.RelationshipLockHelper.lockRelationshipsInOrder;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;
import static org.neo4j.kernel.impl.store.record.Record.isNull;
import static org.neo4j.kernel.impl.store.record.RecordLoad.ALWAYS;
import static org.neo4j.lock.ResourceTypes.NODE;
import static org.neo4j.lock.ResourceTypes.NODE_RELATIONSHIP_GROUP_DELETE;
import static org.neo4j.lock.ResourceTypes.RELATIONSHIP;
import static org.neo4j.lock.ResourceTypes.RELATIONSHIP_GROUP;

/**
 * Manages locking and creation/delete of relationships. Will call on {@link RelationshipCreator} and {@link RelationshipDeleter} for actual
 * record changes in the end.
 */
public class RelationshipModifier
{
    public static final int DEFAULT_EXTERNAL_DEGREES_THRESHOLD_SWITCH = 10;

    private final RelationshipGroupGetter relGroupGetter;
    private final int denseNodeThreshold;
    private final PageCursorTracer cursorTracer;
    private final MemoryTracker memoryTracker;
    private final RelationshipCreator creator;
    private final RelationshipDeleter deleter;

    public RelationshipModifier( RelationshipGroupGetter relGroupGetter, PropertyDeleter propertyChainDeleter, int denseNodeThreshold,
            boolean relaxedLockingForDenseNodes, PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
    {
        this.relGroupGetter = relGroupGetter;
        this.denseNodeThreshold = denseNodeThreshold;
        this.cursorTracer = cursorTracer;
        this.memoryTracker = memoryTracker;
        long externalDegreesThreshold = relaxedLockingForDenseNodes ? DEFAULT_EXTERNAL_DEGREES_THRESHOLD_SWITCH : Long.MAX_VALUE;
        this.creator = new RelationshipCreator( denseNodeThreshold, externalDegreesThreshold, cursorTracer );
        this.deleter = new RelationshipDeleter( relGroupGetter, propertyChainDeleter, externalDegreesThreshold, cursorTracer );
    }

    public void modifyRelationships( RelationshipModifications modifications, RecordAccessSet recordChanges,
            RelationshipGroupDegreesStore.Updater groupDegreesUpdater, ResourceLocker locks, LockTracer lockTracer )
    {
        try ( HeapTrackingLongObjectHashMap<NodeContext> contexts = newLongObjectMap( memoryTracker ) )
        {
            MappedNodeDataLookup nodeDataLookup = new MappedNodeDataLookup( contexts, relGroupGetter, recordChanges, cursorTracer, memoryTracker );
            // Acquire most locks
            acquireMostOfTheNodeAndGroupsLocks( modifications, recordChanges, locks, lockTracer, contexts, nodeDataLookup );
            acquireRelationshipLocksAndSomeOthers( modifications, recordChanges, locks, lockTracer, contexts );

            // Do the modifications
            creator.relationshipCreate( modifications.creations(), recordChanges, groupDegreesUpdater, nodeDataLookup );
            deleter.relationshipDelete( modifications.deletions(), recordChanges, groupDegreesUpdater, nodeDataLookup,
                    locks /*no lock tracing because no blocking acquisitions*/ );
        }
    }

    private void acquireMostOfTheNodeAndGroupsLocks( RelationshipModifications modifications, RecordAccessSet recordChanges, ResourceLocker locks,
            LockTracer lockTracer, MutableLongObjectMap<NodeContext> contexts, MappedNodeDataLookup nodeDataLookup )
    {
        modifications.forEachSplit( byNode ->
        {
            long nodeId = byNode.nodeId();
            RecordProxy<NodeRecord,Void> nodeProxy = recordChanges.getNodeRecords().getOrLoad( nodeId, null, cursorTracer );
            NodeRecord node = nodeProxy.forReadingLinkage();
            boolean nodeIsAddedInTx = node.isCreated();
            if ( !node.isDense() )
            {
                if ( !nodeIsAddedInTx ) // to avoid locking unnecessarily
                {
                    locks.acquireExclusive( lockTracer, NODE, nodeId );
                    nodeProxy = recordChanges.getNodeRecords().getOrLoad( nodeId, null, cursorTracer );
                    node = nodeProxy.forReadingLinkage();
                    if ( node.isDense() )
                    {
                        //another tx just turned dense, unlock and let it be handled below
                        locks.releaseExclusive( NODE, nodeId );
                    }
                    else if ( byNode.hasCreations() )
                    {
                        // The creation code after locking might turn this node dense, at which point the group lock will be needed, so lock it
                        locks.acquireExclusive( lockTracer, RELATIONSHIP_GROUP, nodeId );
                    }
                }
            }

            if ( node.isDense() )
            {
                locks.acquireShared( lockTracer, RELATIONSHIP_GROUP, nodeId ); //stabilize first in chains, in case they are deleted or needed for chain degrees
                // Creations
                NodeContext nodeContext = NodeContext.createNodeContext( nodeProxy, memoryTracker );
                contexts.put( nodeId, nodeContext );
                if ( byNode.hasCreations() )
                {
                    byNode.forEachCreationSplit( byType ->
                    {
                        RelationshipGroupGetter.RelationshipGroupPosition groupPosition = relGroupGetter.getRelationshipGroup( nodeContext.groupStartingPrevId,
                                nodeContext.groupStartingId, byType.type(), recordChanges.getRelGroupRecords(), group ->
                                {
                                    if ( group.getType() != byType.type() )
                                    {
                                        nodeContext.checkEmptyGroup( group );
                                    }
                                } );
                        nodeContext.setCurrentGroup( groupPosition.group() != null ? groupPosition.group() : groupPosition.closestPrevious() );
                        RecordProxy<RelationshipGroupRecord,Integer> groupProxy = groupPosition.group();
                        if ( groupProxy == null )
                        {
                            if ( !nodeContext.hasExclusiveGroupLock() )
                            {
                                locks.releaseShared( RELATIONSHIP_GROUP, nodeId );
                                locks.acquireExclusive( lockTracer, NODE, nodeId );
                                locks.acquireExclusive( lockTracer, RELATIONSHIP_GROUP, nodeId );
                            }
                            nodeContext.setNode( recordChanges.getNodeRecords().getOrLoad( nodeId, null, cursorTracer ) );
                            long groupStartingId = nodeContext.node().forReadingLinkage().getNextRel();
                            long groupStartingPrevId = NULL_REFERENCE.longValue();
                            if ( groupPosition.closestPrevious() != null )
                            {
                                groupStartingId = groupPosition.closestPrevious().getKey();
                                groupStartingPrevId = groupPosition.closestPrevious().forReadingLinkage().getPrev();
                            }
                            groupProxy = relGroupGetter.getOrCreateRelationshipGroup( nodeContext.node(), byType.type(), recordChanges.getRelGroupRecords(),
                                    groupStartingPrevId, groupStartingId );
                            if ( !nodeContext.hasExclusiveGroupLock() )
                            {
                                nodeContext.markExclusiveGroupLock();
                            }
                            else if ( groupProxy.isCreated() )
                            {
                                nodeContext.clearDenseContext(); //When a new group is created we can no longer trust the pointers of the cache
                            }
                        }
                        nodeContext.denseContext( byType.type() ).setGroup( groupProxy );
                    } );

                    if ( !nodeContext.hasExclusiveGroupLock() )
                    {
                        byNode.forEachCreationSplitInterruptible( byType ->
                        {
                            RelationshipGroupRecord group = nodeContext.denseContext( byType.type() ).group().forReadingLinkage();
                            if ( byType.hasOut() && (!group.hasExternalDegreesOut() || isNull( group.getFirstOut() ) )
                                    || byType.hasIn() && (!group.hasExternalDegreesIn() || isNull( group.getFirstIn() ) )
                                    || byType.hasLoop() && (!group.hasExternalDegreesLoop() || isNull( group.getFirstLoop() ) ) )
                            {
                                locks.releaseShared( RELATIONSHIP_GROUP, nodeId );
                                locks.acquireExclusive( lockTracer, RELATIONSHIP_GROUP, nodeId );
                                nodeContext.markExclusiveGroupLock();
                                return true;
                            }
                            return false;
                        } );
                    }
                }

                // Deletions
                if ( byNode.hasDeletions() )
                {
                    if ( !nodeContext.hasExclusiveGroupLock() )
                    {
                        byNode.forEachDeletionSplitInterruptible( byType ->
                        {
                            NodeContext.DenseContext denseContext = nodeContext.denseContext( byType.type() );
                            RelationshipGroupRecord group = denseContext.getOrLoadGroup( relGroupGetter, nodeContext.node().forReadingLinkage(), byType.type(),
                                    recordChanges.getRelGroupRecords(), cursorTracer );
                            if ( byType.hasOut() && !group.hasExternalDegreesOut()
                                    || byType.hasIn() && !group.hasExternalDegreesIn()
                                    || byType.hasLoop() && !group.hasExternalDegreesLoop() )
                            {
                                locks.releaseShared( RELATIONSHIP_GROUP, nodeId );
                                locks.acquireExclusive( lockTracer, RELATIONSHIP_GROUP, nodeId );
                                nodeContext.markExclusiveGroupLock();
                                return true;
                            }
                            else
                            {
                                boolean hasAnyFirst = batchContains( byType.out(), group.getFirstOut() )
                                        || batchContains( byType.in(), group.getFirstIn() )
                                        || batchContains( byType.loop(), group.getFirstLoop() );
                                if ( hasAnyFirst )
                                {
                                    locks.releaseShared( RELATIONSHIP_GROUP, nodeId );
                                    locks.acquireExclusive( lockTracer, RELATIONSHIP_GROUP, nodeId );
                                    nodeContext.markExclusiveGroupLock();
                                    return true;
                                }
                            }
                            return false;
                        } );
                    }
                }

                // Look for an opportunity to delete empty groups that we noticed while looking for groups above
                if ( nodeContext.hasExclusiveGroupLock() && nodeContext.hasAnyEmptyGroup() )
                {
                    // There may to be one or more empty groups that we can delete
                    if ( locks.tryExclusiveLock( NODE_RELATIONSHIP_GROUP_DELETE, nodeId ) )
                    {
                        // We got the EXCLUSIVE group lock so we can go ahead and try to remove any potentially empty groups
                        if ( !nodeContext.hasEmptyFirstGroup() || locks.tryExclusiveLock( NODE, nodeId ) )
                        {
                            if ( nodeContext.hasEmptyFirstGroup() )
                            {
                                // It's possible that we need to delete the first group, i.e. we just now locked the node and therefore need to re-read it
                                nodeContext.setNode( recordChanges.getNodeRecords().getOrLoad( nodeId, null, cursorTracer ) );
                            }
                            Predicate<RelationshipGroupRecord> canDeleteGroup = group -> !byNode.hasCreations( group.getType() );
                            if ( relGroupGetter.deleteEmptyGroups( nodeContext.node(), canDeleteGroup, nodeDataLookup ) )
                            {
                                nodeContext.clearDenseContext();
                            }
                        }
                    }
                }
            }
        } );
    }

    private void acquireRelationshipLocksAndSomeOthers( RelationshipModifications modifications, RecordAccessSet recordChanges, ResourceLocker locks,
            LockTracer lockTracer, MutableLongObjectMap<NodeContext> contexts )
    {
        RecordAccess<RelationshipRecord,Void> relRecords = recordChanges.getRelRecords();
        modifications.forEachSplit( byNode ->
        {
            long nodeId = byNode.nodeId();
            NodeRecord node = recordChanges.getNodeRecords().getOrLoad( nodeId, null, cursorTracer ).forReadingLinkage();

            if ( !node.isDense() )
            {
                if ( !checkAndLockRelationshipsIfNodeIsGoingToBeDense( node, byNode, relRecords, locks, lockTracer ) )
                {
                    if ( byNode.hasDeletions() )
                    {
                        lockRelationshipsInOrder( byNode.deletions(), node.getNextRel(), relRecords, locks, cursorTracer, memoryTracker );
                    }
                    else if ( byNode.hasCreations() )
                    {
                        //First rel is taken if we don't have any deletions!
                        long firstRel = node.getNextRel();
                        if ( !isNull( firstRel ) )
                        {
                            locks.acquireExclusive( lockTracer, RELATIONSHIP, firstRel );
                        }
                    }
                }
            }
            else
            {
                NodeContext nodeContext = contexts.get( nodeId );
                if ( byNode.hasDeletions() )
                {
                    byNode.forEachDeletionSplit( byType ->
                    {
                        NodeContext.DenseContext context = nodeContext.denseContext( byType.type() );
                        RelationshipGroupRecord group =
                                context.getOrLoadGroup( relGroupGetter, node, byType.type(), recordChanges.getRelGroupRecords(), cursorTracer );
                        long outFirstInChainForDegrees = !group.hasExternalDegreesOut() ? group.getFirstOut() : NULL_REFERENCE.longValue();
                        long inFirstInChainForDegrees = !group.hasExternalDegreesIn() ? group.getFirstIn() : NULL_REFERENCE.longValue();
                        long loopFirstInChainForDegrees = !group.hasExternalDegreesLoop() ? group.getFirstLoop() : NULL_REFERENCE.longValue();
                        lockRelationshipsInOrder( byType.out(), outFirstInChainForDegrees, relRecords, locks, cursorTracer, memoryTracker );
                        lockRelationshipsInOrder( byType.in(), inFirstInChainForDegrees, relRecords, locks, cursorTracer, memoryTracker );
                        lockRelationshipsInOrder( byType.loop(), loopFirstInChainForDegrees, relRecords, locks, cursorTracer, memoryTracker );

                        context.setEntryPoint( DIR_OUT, entrypointFromDeletion( byType.out(), relRecords ) );
                        context.setEntryPoint( DIR_IN, entrypointFromDeletion( byType.in(), relRecords ) );
                        context.setEntryPoint( DIR_LOOP, entrypointFromDeletion( byType.loop(), relRecords ) );
                    } );
                }

                if ( byNode.hasCreations() )
                {
                    byNode.forEachCreationSplit( byType ->
                    {
                        NodeContext.DenseContext context = nodeContext.denseContext( byType.type() );
                        RelationshipGroupRecord group =
                                context.getOrLoadGroup( relGroupGetter, node, byType.type(), recordChanges.getRelGroupRecords(), cursorTracer );
                        context.setEntryPoint( DIR_OUT, findAndLockEntrypointForDense(
                                byType.out(), context.entryPoint( DIR_OUT ), relRecords, locks, lockTracer, group, DirectionWrapper.OUTGOING, nodeId ) );
                        context.setEntryPoint( DIR_IN, findAndLockEntrypointForDense(
                                byType.in(), context.entryPoint( DIR_IN ), relRecords, locks, lockTracer, group, DirectionWrapper.INCOMING, nodeId ) );
                        context.setEntryPoint( DIR_LOOP, findAndLockEntrypointForDense(
                                byType.loop(), context.entryPoint( DIR_LOOP ), relRecords, locks, lockTracer, group, DirectionWrapper.LOOP, nodeId ) );
                        context.markEntryPointsAsChanged();
                    } );
                }

                if ( !nodeContext.hasExclusiveGroupLock() )
                {
                    // we no longer need the read lock, as all we either locked the first in chains, or have exclusive lock
                    locks.releaseShared( RELATIONSHIP_GROUP, byNode.nodeId() );
                }
            }
        } );
    }

    private boolean checkAndLockRelationshipsIfNodeIsGoingToBeDense( NodeRecord node, RelationshipModifications.NodeRelationshipIds byNode,
            RecordAccess<RelationshipRecord,Void> relRecords, ResourceLocker locks, LockTracer lockTracer )
    {
        long nextRel = node.getNextRel();
        if ( !isNull( nextRel ) )
        {
            RelationshipRecord rel = relRecords.getOrLoad( nextRel, null, cursorTracer ).forReadingData();
            long nodeId = node.getId();
            if ( !rel.isFirstInChain( nodeId ) )
            {
                throw new IllegalStateException( "Expected node " + rel + " to be first in chain for node " + nodeId );
            }
            int currentDegree = relCount( nodeId, rel );
            if ( currentDegree + byNode.creations().size() >= denseNodeThreshold )
            {
                long[] ids = new long[currentDegree];
                int index = 0;
                do
                {
                    ids[index++] = nextRel;
                    nextRel = relRecords.getOrLoad( nextRel, null, cursorTracer ).forReadingData().getNextRel( nodeId );
                }
                while ( !isNull( nextRel ) );

                if ( index != currentDegree )
                {
                    throw new IllegalStateException( "Degree " + currentDegree + " for " + node + " doesn't match actual relationship count " + index );
                }
                Arrays.sort( ids );
                locks.acquireExclusive( lockTracer, RELATIONSHIP, ids );
                return true;
            }
        }
        return false;
    }

    private RecordProxy<RelationshipRecord,Void> entrypointFromDeletion( RelationshipBatch deletions, RecordAccess<RelationshipRecord,Void> relRecords )
    {
        return deletions.isEmpty() ? null : relRecords.getOrLoad( deletions.first(), null, ALWAYS, cursorTracer );
    }

    private RecordProxy<RelationshipRecord,Void> findAndLockEntrypointForDense( RelationshipBatch creations,
            RecordProxy<RelationshipRecord,Void> potentialEntrypointFromDeletion, RecordAccess<RelationshipRecord,Void> relRecords, ResourceLocker locks,
            LockTracer lockTracer, RelationshipGroupRecord group, DirectionWrapper direction, long nodeId )
    {
        if ( !creations.isEmpty() )
        {
            if ( potentialEntrypointFromDeletion != null )
            {
                return potentialEntrypointFromDeletion;
            }
            // If we get here then there are no deletions on this chain and we have the RELATIONSHIP_GROUP SHARED Lock
            long firstInChain = direction.getNextRel( group );
            if ( !isNull( firstInChain ) )
            {
                if ( !direction.hasExternalDegrees( group ) )
                {
                    locks.acquireExclusive( lockTracer, RELATIONSHIP, firstInChain );
                }
                return findAndLockEntrypoint( firstInChain, nodeId, relRecords, locks, lockTracer, cursorTracer );
            }
        }
        return null;
    }

    private static boolean batchContains( RelationshipBatch batch, long id )
    {
        return !isNull( id ) && batch.contains( id );
    }
}

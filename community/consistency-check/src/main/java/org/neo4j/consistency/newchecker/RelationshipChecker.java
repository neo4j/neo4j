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
package org.neo4j.consistency.newchecker;

import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.cache.CacheSlots;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.recordstorage.RecordRelationshipScanCursor;
import org.neo4j.internal.recordstorage.RecordStorageReader;
import org.neo4j.internal.recordstorage.RelationshipCounter;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.storable.Value;

import static org.neo4j.common.EntityType.RELATIONSHIP;
import static org.neo4j.consistency.newchecker.RecordLoading.checkValidToken;
import static org.neo4j.consistency.newchecker.RecordLoading.lightClear;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;

/**
 * Checks relationships and their properties, type and schema indexes.
 */
class RelationshipChecker implements Checker
{
    private final NeoStores neoStores;
    private final ParallelExecution execution;
    private final ConsistencyReport.Reporter reporter;
    private final CacheAccess cacheAccess;
    private final TokenHolders tokenHolders;
    private final RecordLoading recordLoader;
    private final CountsState observedCounts;
    private final CheckerContext context;
    private final MutableIntObjectMap<MutableIntSet> mandatoryProperties;
    private final List<IndexDescriptor> indexes;
    private final ProgressListener progress;

    RelationshipChecker( CheckerContext context, MutableIntObjectMap<MutableIntSet> mandatoryProperties )
    {
        this.context = context;
        this.neoStores = context.neoStores;
        this.execution = context.execution;
        this.reporter = context.reporter;
        this.cacheAccess = context.cacheAccess;
        this.tokenHolders = context.tokenHolders;
        this.recordLoader = context.recordLoader;
        this.observedCounts = context.observedCounts;
        this.mandatoryProperties = mandatoryProperties;
        this.indexes = context.indexAccessors.onlineRules( RELATIONSHIP );
        this.progress = context.progressReporter( this, "Relationships", neoStores.getRelationshipStore().getHighId() );
    }

    @Override
    public boolean shouldBeChecked( ConsistencyFlags flags )
    {
        return flags.isCheckGraph() || !indexes.isEmpty() && flags.isCheckIndexes();
    }

    @Override
    public void check( LongRange nodeIdRange, boolean firstRange, boolean lastRange ) throws Exception
    {
        execution.run( getClass().getSimpleName() + "-relationships", execution.partition( neoStores.getRelationshipStore(),
                ( from, to, last ) -> () -> check( nodeIdRange, firstRange, from, to ) ) );
        // Let's not report progress for this since it's so much faster than store checks, it's just scanning the cache
        execution.run( getClass().getSimpleName() + "-unusedRelationships", execution.partition( nodeIdRange,
                ( from, to, last ) -> () -> checkNodesReferencingUnusedRelationships( from, to ) ) );
    }

    private void check( LongRange nodeIdRange, boolean firstRound, long fromRelationshipId, long toRelationshipId )
    {
        RelationshipCounter counter = observedCounts.instantiateRelationshipCounter();
        long[] typeHolder = new long[1];
        try ( RecordStorageReader reader = new RecordStorageReader( neoStores );
                RecordRelationshipScanCursor relationshipCursor = reader.allocateRelationshipScanCursor();
                SafePropertyChainReader property = new SafePropertyChainReader( context );
                SchemaComplianceChecker schemaComplianceChecker = new SchemaComplianceChecker( context, mandatoryProperties, indexes ) )
        {
            ProgressListener localProgress = progress.threadLocalReporter();
            CacheAccess.Client client = cacheAccess.client();
            MutableIntObjectMap<Value> propertyValues = new IntObjectHashMap<>();
            for ( long relationshipId = fromRelationshipId; relationshipId < toRelationshipId && !context.isCancelled(); relationshipId++ )
            {
                localProgress.add( 1 );
                relationshipCursor.single( relationshipId );
                if ( !relationshipCursor.next() )
                {
                    continue;
                }

                // Start/end nodes
                long startNode = relationshipCursor.getFirstNode();
                boolean startNodeIsWithinRange = nodeIdRange.isWithinRangeExclusiveTo( startNode );
                boolean startNodeIsNegativeOnFirstRound = startNode < 0 && firstRound;
                if ( startNodeIsWithinRange || startNodeIsNegativeOnFirstRound )
                {
                    checkRelationshipVsNode( client, relationshipCursor, startNode, relationshipCursor.isFirstInFirstChain(),
                            ( relationship, node ) -> reporter.forRelationship( relationship ).sourceNodeNotInUse( node ),
                            ( relationship, node ) -> reporter.forRelationship( relationship ).sourceNodeDoesNotReferenceBack( node ),
                            ( relationship, node ) -> reporter.forNode( node ).relationshipNotFirstInSourceChain( relationship ),
                            ( relationship, node ) -> reporter.forRelationship( relationship ).sourceNodeHasNoRelationships( node ),
                            relationship -> reporter.forRelationship( relationship ).illegalSourceNode() );
                }
                long endNode = relationshipCursor.getSecondNode();
                boolean endNodeIsWithinRange = nodeIdRange.isWithinRangeExclusiveTo( endNode );
                boolean endNodeIsNegativeOnFirstRound = endNode < 0 && firstRound;
                if ( endNodeIsWithinRange || endNodeIsNegativeOnFirstRound )
                {
                    checkRelationshipVsNode( client, relationshipCursor, endNode, relationshipCursor.isFirstInSecondChain(),
                            ( relationship, node ) -> reporter.forRelationship( relationship ).targetNodeNotInUse( node ),
                            ( relationship, node ) -> reporter.forRelationship( relationship ).targetNodeDoesNotReferenceBack( node ),
                            ( relationship, node ) -> reporter.forNode( node ).relationshipNotFirstInTargetChain( relationship ),
                            ( relationship, node ) -> reporter.forRelationship( relationship ).targetNodeHasNoRelationships( node ),
                            relationship -> reporter.forRelationship( relationship ).illegalTargetNode() );
                }

                if ( firstRound )
                {
                    if ( startNode >= context.highNodeId )
                    {
                        reporter.forRelationship( relationshipCursor ).sourceNodeNotInUse( context.recordLoader.node( startNode ) );
                    }

                    if ( endNode >= context.highNodeId )
                    {
                        reporter.forRelationship( relationshipCursor ).targetNodeNotInUse( context.recordLoader.node( endNode ) );
                    }

                    // Properties
                    typeHolder[0] = relationshipCursor.getType();
                    lightClear( propertyValues );
                    boolean propertyChainIsOk = property.read( propertyValues, relationshipCursor, reporter::forRelationship );
                    if ( propertyChainIsOk )
                    {
                        schemaComplianceChecker.checkContainsMandatoryProperties( relationshipCursor, typeHolder, propertyValues, reporter::forRelationship );
                        if ( context.consistencyFlags.isCheckIndexes() )
                        {
                            schemaComplianceChecker.checkCorrectlyIndexed( relationshipCursor, typeHolder, propertyValues, reporter::forRelationship );
                        }
                    }

                    // Type and count
                    checkValidToken( relationshipCursor, relationshipCursor.type(), tokenHolders.relationshipTypeTokens(),
                            neoStores.getRelationshipTypeTokenStore(), ( rel, token ) -> reporter.forRelationship( rel ).illegalRelationshipType(),
                            ( rel, token ) -> reporter.forRelationship( rel ).relationshipTypeNotInUse( token ) );
                    observedCounts.incrementRelationshipTypeCounts( counter, relationshipCursor );
                }
                observedCounts.incrementRelationshipNodeCounts( counter, relationshipCursor, startNodeIsWithinRange, endNodeIsWithinRange );
            }
            localProgress.done();
        }
    }

    private void checkRelationshipVsNode( CacheAccess.Client client, RecordRelationshipScanCursor relationshipCursor, long node, boolean firstInChain,
            BiConsumer<RelationshipRecord,NodeRecord> reportNodeNotInUse,
            BiConsumer<RelationshipRecord,NodeRecord> reportNodeDoesNotReferenceBack,
            BiConsumer<RelationshipRecord,NodeRecord> reportNodeNotFirstInChain,
            BiConsumer<RelationshipRecord,NodeRecord> reportNodeHasNoChain,
            Consumer<RelationshipRecord> reportIllegalNode )
    {
        // Check validity of node reference
        if ( node < 0 )
        {
            reportIllegalNode.accept( recordLoader.relationship( relationshipCursor.getId() ) );
            return;
        }

        // Check if node is in use
        boolean nodeInUse = client.getBooleanFromCache( node, CacheSlots.NodeLink.SLOT_IN_USE );
        if ( !nodeInUse )
        {
            reportNodeNotInUse.accept( recordLoader.relationship( relationshipCursor.getId() ), recordLoader.node( node ) );
            return;
        }

        // Check if node has nextRel reference at all
        long nodeNextRel = client.getFromCache( node, CacheSlots.NodeLink.SLOT_RELATIONSHIP_ID );
        if ( NULL_REFERENCE.is( nodeNextRel ) )
        {
            reportNodeHasNoChain.accept( recordLoader.relationship( relationshipCursor.getId() ), recordLoader.node( node ) );
            return;
        }

        // Check the node <--> relationship references
        boolean nodeIsDense = client.getBooleanFromCache( node, CacheSlots.NodeLink.SLOT_IS_DENSE );
        if ( !nodeIsDense )
        {
            if ( firstInChain )
            {
                if ( nodeNextRel != relationshipCursor.getId() )
                {
                    // Report RELATIONSHIP -> NODE inconsistency
                    reportNodeDoesNotReferenceBack.accept( recordLoader.relationship( relationshipCursor.getId() ), recordLoader.node( node ) );
                    // Before marking this node as fully checked we should also check and report any NODE -> RELATIONSHIP inconsistency
                    RelationshipRecord relationshipThatNodeActuallyReferences = recordLoader.relationship( nodeNextRel );
                    if ( !relationshipThatNodeActuallyReferences.inUse() )
                    {
                        reporter.forNode( recordLoader.node( node ) ).relationshipNotInUse( relationshipThatNodeActuallyReferences );
                    }
                    else if ( relationshipThatNodeActuallyReferences.getFirstNode() != node && relationshipThatNodeActuallyReferences.getSecondNode() != node )
                    {
                        reporter.forNode( recordLoader.node( node ) ).relationshipForOtherNode( relationshipThatNodeActuallyReferences );
                    }
                }
                client.putToCacheSingle( node, CacheSlots.NodeLink.SLOT_CHECK_MARK, 0 );
            }
            if ( !firstInChain && nodeNextRel == relationshipCursor.getId() )
            {
                reportNodeNotFirstInChain.accept( recordLoader.relationship( relationshipCursor.getId() ), recordLoader.node( node ) );
            }
        }
    }

    private void checkNodesReferencingUnusedRelationships( long fromNodeId, long toNodeId )
    {
        // Do this after we've done node.nextRel caching and checking of those. Checking also clears those values, so simply
        // go through the cache and see if there are any relationship ids left and report them
        CacheAccess.Client client = cacheAccess.client();
        for ( long id = fromNodeId; id < toNodeId && !context.isCancelled(); id++ )
        {
            // Only check if we haven't come across this sparse node while checking relationships
            boolean nodeInUse = client.getBooleanFromCache( id, CacheSlots.NodeLink.SLOT_IN_USE );
            if ( nodeInUse )
            {
                boolean needsChecking = client.getBooleanFromCache( id, CacheSlots.NodeLink.SLOT_CHECK_MARK );
                if ( needsChecking )
                {
                    long nodeNextRel = client.getFromCache( id, CacheSlots.NodeLink.SLOT_RELATIONSHIP_ID );
                    boolean nodeIsDense = client.getBooleanFromCache( id, CacheSlots.NodeLink.SLOT_IS_DENSE );
                    if ( !NULL_REFERENCE.is( nodeNextRel ) )
                    {
                        if ( !nodeIsDense )
                        {
                            RelationshipRecord relationship = recordLoader.relationship( nodeNextRel );
                            NodeRecord node = recordLoader.node( id );
                            if ( !relationship.inUse() )
                            {
                                reporter.forNode( node ).relationshipNotInUse( relationship );
                            }
                            else
                            {
                                reporter.forNode( node ).relationshipForOtherNode( relationship );
                            }
                        }
                        else
                        {
                            RelationshipGroupRecord group = recordLoader.relationshipGroup( nodeNextRel );
                            if ( !group.inUse() )
                            {
                                reporter.forNode( recordLoader.node( id ) ).relationshipGroupNotInUse( group );
                            }
                            else
                            {
                                reporter.forNode( recordLoader.node( id ) ).relationshipGroupHasOtherOwner( group );
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    public String toString()
    {
        return String.format( "%s[highId:%d,indexesToCheck:%d]", getClass().getSimpleName(), neoStores.getRelationshipStore().getHighId(), indexes.size() );
    }
}

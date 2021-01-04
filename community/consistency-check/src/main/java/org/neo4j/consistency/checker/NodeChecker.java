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
package org.neo4j.consistency.checker;

import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.util.Iterator;
import java.util.List;
import java.util.function.LongConsumer;

import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.cache.CacheSlots;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.synthetic.IndexEntry;
import org.neo4j.consistency.store.synthetic.TokenScanDocument;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.recordstorage.RecordNodeCursor;
import org.neo4j.internal.recordstorage.RecordStorageReader;
import org.neo4j.internal.recordstorage.RelationshipCounter;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.PropertySchemaType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.impl.index.schema.AllEntriesTokenScanReader;
import org.neo4j.kernel.impl.index.schema.EntityTokenRange;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.storable.Value;

import static java.lang.Math.toIntExact;
import static java.util.Arrays.sort;
import static org.apache.commons.lang3.math.NumberUtils.max;
import static org.apache.commons.lang3.math.NumberUtils.min;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.consistency.checker.RecordLoading.checkValidToken;
import static org.neo4j.consistency.checker.RecordLoading.lightClear;
import static org.neo4j.consistency.checking.cache.CacheSlots.longOf;
import static org.neo4j.kernel.impl.store.record.Record.NO_LABELS_FIELD;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;
import static org.neo4j.token.api.TokenConstants.ANY_LABEL;

/**
 * Checks nodes and their properties, labels and schema and label indexes.
 */
class NodeChecker implements Checker
{
    private static final String NODE_INDEXES_CHECKER_TAG = "nodeIndexesChecker";
    private static final String NODE_RANGE_CHECKER_TAG = "nodeRangeChecker";
    private final MutableIntObjectMap<MutableIntSet> mandatoryProperties;
    private final ProgressListener nodeProgress;
    private final CheckerContext context;
    private final ConsistencyReport.Reporter reporter;
    private final CountsState observedCounts;
    private final RecordLoading recordLoader;
    private final TokenHolders tokenHolders;
    private final NeoStores neoStores;
    private final List<IndexDescriptor> smallIndexes;

    NodeChecker( CheckerContext context, MutableIntObjectMap<MutableIntSet> mandatoryProperties )
    {
        this.context = context;
        this.reporter = context.reporter;
        this.observedCounts = context.observedCounts;
        this.recordLoader = context.recordLoader;
        this.tokenHolders = context.tokenHolders;
        this.neoStores = context.neoStores;
        this.mandatoryProperties = mandatoryProperties;
        this.nodeProgress = context.roundInsensitiveProgressReporter( this, "Nodes", neoStores.getNodeStore().getHighId() );
        this.smallIndexes = context.indexSizes.smallIndexes( NODE );
    }

    @Override
    public void check( LongRange nodeIdRange, boolean firstRange, boolean lastRange ) throws Exception
    {
        ParallelExecution execution = context.execution;
        execution.run( getClass().getSimpleName() + "-checkNodes", execution.partition( nodeIdRange,
                ( from, to, last ) -> () -> check( from, to, lastRange && last ) ) );

        if ( context.consistencyFlags.isCheckIndexes() )
        {
            execution.run( getClass().getSimpleName() + "-checkIndexesVsNodes", smallIndexes.stream()
                    .map( indexDescriptor -> (ParallelExecution.ThrowingRunnable) () -> checkIndexVsNodes( nodeIdRange, indexDescriptor, lastRange ) )
                    .toArray( ParallelExecution.ThrowingRunnable[]::new ) );
        }
    }

    @Override
    public boolean shouldBeChecked( ConsistencyFlags flags )
    {
        return flags.isCheckGraph() || flags.isCheckIndexes() && !smallIndexes.isEmpty();
    }

    private void check( long fromNodeId, long toNodeId, boolean last ) throws Exception
    {
        long usedNodes = 0;
        try ( RecordStorageReader reader = new RecordStorageReader( context.neoStores );
              var cursorTracer = context.pageCacheTracer.createPageCursorTracer( NODE_RANGE_CHECKER_TAG );
              RecordNodeCursor nodeCursor = reader.allocateNodeCursor( cursorTracer );
              RecordReader<DynamicRecord> labelReader = new RecordReader<>( context.neoStores.getNodeStore().getDynamicLabelStore(), cursorTracer );
              AllEntriesTokenScanReader labelIndexReader = context.labelScanStore.allEntityTokenRanges( fromNodeId, last ? Long.MAX_VALUE : toNodeId,
                      cursorTracer );
              SafePropertyChainReader property = new SafePropertyChainReader( context, cursorTracer );
              SchemaComplianceChecker schemaComplianceChecker = new SchemaComplianceChecker( context, mandatoryProperties, smallIndexes, cursorTracer,
                      context.memoryTracker ) )
        {
            ProgressListener localProgress = nodeProgress.threadLocalReporter();
            MutableIntObjectMap<Value> propertyValues = new IntObjectHashMap<>();
            CacheAccess.Client client = context.cacheAccess.client();
            long[] nextRelCacheFields = new long[]{-1, -1, 1/*inUse*/, 0, 0, 1/*note that this needs to be checked*/, 0};
            Iterator<EntityTokenRange> nodeLabelRangeIterator = labelIndexReader.iterator();
            EntityTokenIndexCheckState labelIndexState = new EntityTokenIndexCheckState( null, fromNodeId - 1 );
            for ( long nodeId = fromNodeId; nodeId < toNodeId && !context.isCancelled(); nodeId++ )
            {
                localProgress.add( 1 );
                nodeCursor.single( nodeId );
                nodeCursor.setForceLoad();
                if ( !nodeCursor.next() )
                {
                    continue;
                }

                // Cache nextRel
                long nextRel = nodeCursor.getNextRel();
                if ( nextRel < NULL_REFERENCE.longValue() )
                {
                    reporter.forNode( nodeCursor ).relationshipNotInUse( new RelationshipRecord( nextRel ) );
                    nextRel = NULL_REFERENCE.longValue();
                }

                nextRelCacheFields[CacheSlots.NodeLink.SLOT_RELATIONSHIP_ID] = nextRel;
                nextRelCacheFields[CacheSlots.NodeLink.SLOT_IS_DENSE] = longOf( nodeCursor.isDense() );
                usedNodes++;

                // Labels
                long[] unverifiedLabels = RecordLoading.safeGetNodeLabels( context, nodeCursor.getId(), nodeCursor.getLabelField(), labelReader, cursorTracer );
                long[] labels = checkNodeLabels( nodeCursor, unverifiedLabels, cursorTracer );
                // Cache the label field, so that if it contains inlined labels then it's free.
                // Otherwise cache the dynamic labels in another data structure and point into it.
                long labelField = nodeCursor.getLabelField();
                boolean hasInlinedLabels = !NodeLabelsField.fieldPointsToDynamicRecordOfLabels( nodeCursor.getLabelField() );
                if ( labels == null )
                {
                    // There was some inconsistency in the label field or dynamic label chain. Let's continue but w/o labels for this node
                    hasInlinedLabels = true;
                    labelField = NO_LABELS_FIELD.longValue();
                }
                boolean hasSingleLabel = labels != null && labels.length == 1;
                nextRelCacheFields[CacheSlots.NodeLink.SLOT_HAS_INLINED_LABELS] = longOf( hasInlinedLabels );
                nextRelCacheFields[CacheSlots.NodeLink.SLOT_LABELS] = hasSingleLabel
                        // If this node has only a single label then put it straight in there w/o encoding, along w/ SLOT_HAS_SINGLE_LABEL=1
                        // this makes RelationshipChecker "parse" the cached node labels more efficiently for single-label nodes
                        ? labels[0]
                        // Otherwise put the encoded label field if inlined, otherwise a ref to the cached dynamic labels
                        : hasInlinedLabels ? labelField : observedCounts.cacheDynamicNodeLabels( labels );
                nextRelCacheFields[CacheSlots.NodeLink.SLOT_HAS_SINGLE_LABEL] = longOf( hasSingleLabel );

                // Properties
                lightClear( propertyValues );
                boolean propertyChainIsOk = property.read( propertyValues, nodeCursor, reporter::forNode, cursorTracer );

                // Label index
                checkNodeVsLabelIndex( nodeCursor, nodeLabelRangeIterator, labelIndexState, nodeId, labels, fromNodeId, cursorTracer );
                client.putToCache( nodeId, nextRelCacheFields );

                // Mandatory properties and (some) indexing
                if ( labels != null && propertyChainIsOk )
                {
                    schemaComplianceChecker.checkContainsMandatoryProperties( nodeCursor, labels, propertyValues, reporter::forNode );
                    // Here only the very small indexes (or indexes that we can't read the values from, like fulltext indexes)
                    // gets checked this way, larger indexes will be checked in IndexChecker
                    if ( context.consistencyFlags.isCheckIndexes() )
                    {
                        schemaComplianceChecker.checkCorrectlyIndexed( nodeCursor, labels, propertyValues, reporter::forNode );
                    }
                }
                // Large indexes are checked elsewhere, more efficiently than per-entity
            }
            if ( !context.isCancelled() )
            {
                reportRemainingLabelIndexEntries( nodeLabelRangeIterator, labelIndexState, last ? Long.MAX_VALUE : toNodeId, cursorTracer );
            }
            localProgress.done();
        }
        observedCounts.incrementNodeLabel( ANY_LABEL, usedNodes );
    }

    private long[] checkNodeLabels( RecordNodeCursor nodeCursor, long[] labels, PageCursorTracer cursorTracer )
    {
        if ( labels == null )
        {
            // Because there was something wrong with loading them
            return null;
        }

        boolean allGood = true;
        boolean valid = true;
        int prevLabel = -1;
        for ( int i = 0; i < labels.length; i++ )
        {
            long longLabel = labels[i];
            if ( longLabel > Integer.MAX_VALUE )
            {
                reporter.forNode( recordLoader.node( nodeCursor.getId(), cursorTracer ) ).illegalLabel();
                allGood = false;
                valid = false;
                break;
            }
            else
            {
                int label = toIntExact( longLabel );
                checkValidToken( nodeCursor, label, tokenHolders.labelTokens(), neoStores.getLabelTokenStore(),
                        ( node, token ) -> reporter.forNode( recordLoader.node( node.getId(), cursorTracer ) ).illegalLabel(),
                        ( node, token ) -> reporter.forNode( recordLoader.node( node.getId(), cursorTracer ) ).labelNotInUse( token ), cursorTracer );
                if ( prevLabel != label )
                {
                    observedCounts.incrementNodeLabel( label, 1 );
                    prevLabel = label;
                }
            }

            if ( i > 0 )
            {
                if ( labels[i] == labels[i - 1] )
                {
                    reporter.forNode( nodeCursor ).labelDuplicate( labels[i] );
                    allGood = false;
                    break;
                }
                else if ( labels[i] < labels[i - 1] )
                {
                    reporter.forNode( nodeCursor ).labelsOutOfOrder( max( labels ), min( labels ) );
                    allGood = false;
                    break;
                }
            }
        }

        if ( !valid )
        {
            return null;
        }
        return allGood ? labels : sortAndDeduplicate( labels );
    }

    private void checkNodeVsLabelIndex( RecordNodeCursor nodeCursor, Iterator<EntityTokenRange> nodeLabelRangeIterator,
            EntityTokenIndexCheckState labelIndexState, long nodeId, long[] labels, long fromNodeId, PageCursorTracer cursorTracer )
    {
        // Detect node-label combinations that exist in the label index, but not in the store
        while ( labelIndexState.needToMoveRangeForwardToReachEntity( nodeId ) && !context.isCancelled() )
        {
            if ( nodeLabelRangeIterator.hasNext() )
            {
                if ( labelIndexState.currentRange != null )
                {
                    for ( long nodeIdMissingFromStore = labelIndexState.lastCheckedEntityId + 1;
                          nodeIdMissingFromStore < nodeId & labelIndexState.currentRange.covers( nodeIdMissingFromStore ); nodeIdMissingFromStore++ )
                    {
                        if ( labelIndexState.currentRange.tokens( nodeIdMissingFromStore ).length > 0 )
                        {
                            reporter.forNodeLabelScan( new TokenScanDocument( labelIndexState.currentRange ) ).nodeNotInUse(
                                    recordLoader.node( nodeIdMissingFromStore, cursorTracer ) );
                        }
                    }
                }
                labelIndexState.currentRange = nodeLabelRangeIterator.next();
                labelIndexState.lastCheckedEntityId = max( fromNodeId, labelIndexState.currentRange.entities()[0] ) - 1;
            }
            else
            {
                break;
            }
        }

        if ( labelIndexState.currentRange != null && labelIndexState.currentRange.covers( nodeId ) )
        {
            for ( long nodeIdMissingFromStore = labelIndexState.lastCheckedEntityId + 1; nodeIdMissingFromStore < nodeId; nodeIdMissingFromStore++ )
            {
                if ( labelIndexState.currentRange.tokens( nodeIdMissingFromStore ).length > 0 )
                {
                    reporter.forNodeLabelScan( new TokenScanDocument( labelIndexState.currentRange ) )
                            .nodeNotInUse( recordLoader.node( nodeIdMissingFromStore, cursorTracer ) );
                }
            }
            long[] labelsInLabelIndex = labelIndexState.currentRange.tokens( nodeId );
            if ( labels != null )
            {
                validateLabelIds( nodeCursor, labels, sortAndDeduplicate( labelsInLabelIndex ) /* TODO remove when fixed */, labelIndexState.currentRange,
                        cursorTracer );
            }
            labelIndexState.lastCheckedEntityId = nodeId;
        }
        else if ( labels != null )
        {
            for ( long label : labels )
            {
                reporter.forNodeLabelScan( new TokenScanDocument( new EntityTokenRange( nodeId / Long.SIZE, EntityTokenRange.NO_TOKENS, NODE ) ) )
                        .nodeLabelNotInIndex( recordLoader.node( nodeId, cursorTracer ), label );
            }
        }
    }

    private void reportRemainingLabelIndexEntries( Iterator<EntityTokenRange> nodeLabelRangeIterator, EntityTokenIndexCheckState labelIndexState, long toNodeId,
            PageCursorTracer cursorTracer )
    {
        if ( labelIndexState.currentRange == null && nodeLabelRangeIterator.hasNext() )
        {
            // Seems that nobody touched this iterator before, i.e. no nodes in this whole range
            labelIndexState.currentRange = nodeLabelRangeIterator.next();
        }

        while ( labelIndexState.currentRange != null && !context.isCancelled() )
        {
            for ( long nodeIdMissingFromStore = labelIndexState.lastCheckedEntityId + 1;
                  nodeIdMissingFromStore < toNodeId && !labelIndexState.needToMoveRangeForwardToReachEntity( nodeIdMissingFromStore );
                  nodeIdMissingFromStore++ )
            {
                if ( labelIndexState.currentRange.covers( nodeIdMissingFromStore ) && labelIndexState.currentRange.tokens( nodeIdMissingFromStore ).length > 0 )
                {
                    reporter.forNodeLabelScan( new TokenScanDocument( labelIndexState.currentRange ) )
                            .nodeNotInUse( recordLoader.node( nodeIdMissingFromStore, cursorTracer ) );
                }
                labelIndexState.lastCheckedEntityId = nodeIdMissingFromStore;
            }
            labelIndexState.currentRange = nodeLabelRangeIterator.hasNext() ? nodeLabelRangeIterator.next() : null;
        }
    }

    private void validateLabelIds( NodeRecord node, long[] labelsInStore, long[] labelsInIndex, EntityTokenRange entityTokenRange,
            PageCursorTracer cursorTracer )
    {
        compareTwoSortedLongArrays( PropertySchemaType.COMPLETE_ALL_TOKENS, labelsInStore, labelsInIndex,
                indexLabel -> reporter.forNodeLabelScan( new TokenScanDocument( entityTokenRange ) )
                        .nodeDoesNotHaveExpectedLabel( recordLoader.node( node.getId(), cursorTracer ), indexLabel ),
                storeLabel -> reporter.forNodeLabelScan( new TokenScanDocument( entityTokenRange ) )
                        .nodeLabelNotInIndex( recordLoader.node( node.getId(), cursorTracer ), storeLabel ) );
    }

    static void compareTwoSortedLongArrays( PropertySchemaType propertySchemaType, long[] a, long[] b,
            LongConsumer bHasSomethingThatAIsMissingReport, LongConsumer aHasSomethingThatBIsMissingReport )
    {
        // The node must have all of the labels specified by the index.
        int bCursor = 0;
        int aCursor = 0;
        boolean anyFound = false;
        while ( aCursor < a.length && bCursor < b.length && a[aCursor] != -1 && b[bCursor] != -1 )
        {
            long bValue = b[bCursor];
            long aValue = a[aCursor];

            if ( bValue < aValue )
            {   // node store has a label which isn't in label scan store
                if ( propertySchemaType == PropertySchemaType.COMPLETE_ALL_TOKENS )
                {
                    bHasSomethingThatAIsMissingReport.accept( bValue );
                }
                bCursor++;
            }
            else if ( bValue > aValue )
            {   // label scan store has a label which isn't in node store
                if ( propertySchemaType == PropertySchemaType.COMPLETE_ALL_TOKENS )
                {
                    aHasSomethingThatBIsMissingReport.accept( aValue );
                }
                aCursor++;
            }
            else
            {   // both match
                bCursor++;
                aCursor++;
                anyFound = true;
            }
        }

        if ( propertySchemaType == PropertySchemaType.COMPLETE_ALL_TOKENS )
        {
            while ( bCursor < b.length && b[bCursor] != -1 )
            {
                bHasSomethingThatAIsMissingReport.accept( b[bCursor++] );
            }
            while ( aCursor < a.length && a[aCursor] != -1 )
            {
                aHasSomethingThatBIsMissingReport.accept( a[aCursor++] );
            }
        }
        else if ( propertySchemaType == PropertySchemaType.PARTIAL_ANY_TOKEN )
        {
            if ( !anyFound )
            {
                while ( bCursor < b.length )
                {
                    bHasSomethingThatAIsMissingReport.accept( b[bCursor++] );
                }
            }
        }
    }

    private void checkIndexVsNodes( LongRange range, IndexDescriptor descriptor, boolean lastRange ) throws Exception
    {
        CacheAccess.Client client = context.cacheAccess.client();
        IndexAccessor accessor = context.indexAccessors.accessorFor( descriptor );
        RelationshipCounter.NodeLabelsLookup nodeLabelsLookup = observedCounts.nodeLabelsLookup();
        SchemaDescriptor schema = descriptor.schema();
        PropertySchemaType propertySchemaType = schema.propertySchemaType();
        long[] indexEntityTokenIds = toLongArray( schema.getEntityTokenIds() );
        indexEntityTokenIds = sortAndDeduplicate( indexEntityTokenIds );
        try ( var cursorTracer = context.pageCacheTracer.createPageCursorTracer( NODE_INDEXES_CHECKER_TAG );
              var allEntriesReader = accessor.newAllEntriesReader( range.from(), lastRange ? Long.MAX_VALUE : range.to(), cursorTracer ) )
        {
            for ( long entityId : allEntriesReader )
            {
                try
                {
                    boolean entityExists = client.getBooleanFromCache( entityId, CacheSlots.NodeLink.SLOT_IN_USE );
                    if ( !entityExists )
                    {
                        reporter.forIndexEntry( new IndexEntry( descriptor, context.tokenNameLookup, entityId ) )
                                .nodeNotInUse( recordLoader.node( entityId, cursorTracer ) );
                    }
                    else
                    {
                        long[] entityTokenIds = nodeLabelsLookup.nodeLabels( entityId );
                        compareTwoSortedLongArrays( propertySchemaType, entityTokenIds, indexEntityTokenIds,
                                indexLabel -> reporter.forIndexEntry( new IndexEntry( descriptor, context.tokenNameLookup, entityId ) )
                                        .nodeDoesNotHaveExpectedLabel( recordLoader.node( entityId, cursorTracer ), indexLabel ),
                                storeLabel -> {/*here we're only interested in what the the index has that the store doesn't have*/} );
                    }
                }
                catch ( ArrayIndexOutOfBoundsException e )
                {
                    // OK so apparently the index has a node way outside node highId
                    reporter.forIndexEntry( new IndexEntry( descriptor, context.tokenNameLookup, entityId ) )
                            .nodeNotInUse( recordLoader.node( entityId, cursorTracer ) );
                }
            }
        }
    }

    private static long[] toLongArray( int[] intArray )
    {
        long[] result = new long[intArray.length];
        for ( int i = 0; i < intArray.length; i++ )
        {
            result[i] = intArray[i];
        }
        return result;
    }

    @Override
    public String toString()
    {
        return String.format( "%s[highId:%d,indexesToCheck:%d]", getClass().getSimpleName(), neoStores.getNodeStore().getHighId(), smallIndexes.size() );
    }

    public static long[] sortAndDeduplicate( long[] labels )
    {
        if ( ArrayUtils.isNotEmpty( labels ) )
        {
            sort( labels );
            return PrimitiveLongCollections.deduplicate( labels );
        }
        return labels;
    }
}

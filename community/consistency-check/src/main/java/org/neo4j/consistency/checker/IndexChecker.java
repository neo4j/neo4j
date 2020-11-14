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

import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.common.EntityType;
import org.neo4j.consistency.checker.ParallelExecution.ThrowingRunnable;
import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.synthetic.IndexEntry;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.recordstorage.RecordNodeCursor;
import org.neo4j.internal.recordstorage.RecordStorageReader;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntriesReader;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.neo4j.consistency.checker.RecordLoading.lightClear;
import static org.neo4j.consistency.checker.RecordLoading.safeGetNodeLabels;

public class IndexChecker implements Checker
{
    private static final String INDEX_CHECKER_TAG = "IndexChecker";
    private static final String CONSISTENCY_INDEX_ENTITY_CHECK_TAG = "consistencyIndexEntityCheck";
    private static final String CONSISTENCY_INDEX_CACHER_TAG = "consistencyIndexCacher";
    private static final int INDEX_CACHING_PROGRESS_FACTOR = 3;
    static final int NUM_INDEXES_IN_CACHE = 5;

    private static final int CHECKSUM_MASK = 0b0111_1111_1111_1111;
    private static final int IN_USE_MASK =   0b1000_0000_0000_0000;
    private static final int CHECKSUM_SIZE = 15;
    private static final int IN_USE_BIT = 1;
    private static final int TOTAL_SIZE = CHECKSUM_SIZE + IN_USE_BIT;

    private final EntityType entityType;
    private final ConsistencyReport.Reporter reporter;
    private final CheckerContext context;
    private final IndexAccessors indexAccessors;
    private final CacheAccess cacheAccess;
    private final ProgressListener cacheProgress;
    private final ProgressListener scanProgress;
    private final List<IndexDescriptor> indexes;

    IndexChecker( CheckerContext context, EntityType entityType )
    {
        indexAccessors = context.indexAccessors;
        this.context = context;
        this.entityType = entityType;
        this.reporter = context.reporter;
        this.cacheAccess = context.cacheAccess;
        this.indexes = context.indexSizes.largeIndexes( entityType );
        long totalSize = indexes.stream().mapToLong( context.indexSizes::getEstimatedIndexSize ).sum();
        int rounds = (indexes.size() - 1) / NUM_INDEXES_IN_CACHE + 1;
        this.scanProgress = context.progressReporter( this, "Node index checking", rounds * context.neoStores.getNodeStore().getHighId() );
        // The caching of indexes is generally so much quicker than other things and is based on estimates so dividing by 10
        // makes the progress more even and any estimation flaws less visible.
        this.cacheProgress = context.progressReporter( this, "Node index caching", totalSize / 3 );
    }

    @Override
    public void check( LongRange nodeIdRange, boolean firstRange, boolean lastRange ) throws Exception
    {
        // While more indexes
        //   Scan through one or more indexes (as sequentially as possible) and cache the node ids + hash of the indexed value in one bit-set for each index
        //   Then scan through node store, its labels and relevant properties and hash that value too --> match with the bit-set + hash.

        cacheAccess.setCacheSlotSizesAndClear( TOTAL_SIZE, TOTAL_SIZE, TOTAL_SIZE, TOTAL_SIZE, TOTAL_SIZE ); //can hold up to 5 indexes
        List<IndexContext> indexesToCheck = new ArrayList<>();
        try ( var indexChecker = context.pageCacheTracer.createPageCursorTracer( INDEX_CHECKER_TAG ) )
        {
            for ( int i = 0; i < indexes.size() && !context.isCancelled(); i++ )
            {
                IndexContext index = new IndexContext( indexes.get( i ), i % NUM_INDEXES_IN_CACHE );
                indexesToCheck.add( index );
                cacheIndex( index, nodeIdRange, firstRange, indexChecker );
                boolean isLastIndex = i == indexes.size() - 1;
                boolean canFitMoreAndIsNotLast = !isLastIndex && index.cacheSlotOffset != NUM_INDEXES_IN_CACHE - 1;
                if ( !canFitMoreAndIsNotLast  && !context.isCancelled() )
                {
                    checkVsEntities( indexesToCheck, nodeIdRange );
                    indexesToCheck = new ArrayList<>();
                    cacheAccess.clearCache();
                }
            }
        }
    }

    @Override
    public boolean shouldBeChecked( ConsistencyFlags flags )
    {
        return flags.isCheckIndexes();
    }

    private void cacheIndex( IndexContext index, LongRange nodeIdRange, boolean firstRange, PageCursorTracer cursorTracer ) throws Exception
    {
        IndexAccessor accessor = indexAccessors.accessorFor( index.descriptor );
        IndexEntriesReader[] partitions = accessor.newAllIndexEntriesReader( context.execution.getNumberOfThreads(), cursorTracer );
        try
        {
            Value[][] firstValues = new Value[partitions.length][];
            Value[][] lastValues = new Value[partitions.length][];
            long[] firstEntityIds = new long[partitions.length];
            long[] lastEntityIds = new long[partitions.length];
            ThrowingRunnable[] workers = new ThrowingRunnable[partitions.length];
            for ( int i = 0; i < partitions.length; i++ )
            {
                IndexEntriesReader partition = partitions[i];
                int slot = i;
                workers[i] = () ->
                {
                    int lastChecksum = 0;
                    int progressPart = 0;
                    ProgressListener localCacheProgress = cacheProgress.threadLocalReporter();
                    var client = cacheAccess.client();
                    try ( var tracer = context.pageCacheTracer.createPageCursorTracer( CONSISTENCY_INDEX_CACHER_TAG ) )
                    {
                        while ( partition.hasNext() && !context.isCancelled() )
                        {
                            long entityId = partition.next();
                            if ( !nodeIdRange.isWithinRangeExclusiveTo( entityId ) )
                            {
                                if ( firstRange && entityId >= context.highNodeId )
                                {
                                    reporter.forIndexEntry( new IndexEntry( index.descriptor, context.tokenNameLookup, entityId ) )
                                            .nodeNotInUse( context.recordLoader.node( entityId, tracer ) );
                                }
                                continue;
                            }

                            int data = IN_USE_MASK;
                            if ( index.hasValues )
                            {
                                Value[] indexedValues = partition.values();
                                int checksum = checksum( indexedValues );
                                assert checksum <= CHECKSUM_MASK;
                                data |= checksum;

                                // Also take the opportunity to verify uniqueness, if the index is a uniqueness index
                                if ( index.descriptor.isUnique() )
                                {
                                    if ( firstValues[slot] == null )
                                    {
                                        firstValues[slot] = indexedValues;
                                        firstEntityIds[slot] = entityId;
                                    }
                                    if ( lastValues[slot] != null )
                                    {
                                        if ( lastChecksum == checksum )
                                        {
                                            if ( Arrays.equals( lastValues[slot], indexedValues ) )
                                            {
                                                reporter.forNode( context.recordLoader.node( entityId, tracer ) )
                                                        .uniqueIndexNotUnique( index.descriptor, indexedValues, lastEntityIds[slot] );
                                            }
                                        }
                                    }
                                    lastValues[slot] = indexedValues;
                                    lastChecksum = checksum;
                                    lastEntityIds[slot] = entityId;
                                }
                            }
                            client.putToCacheSingle( entityId, index.cacheSlotOffset, data );
                            if ( ++progressPart == INDEX_CACHING_PROGRESS_FACTOR )
                            {
                                localCacheProgress.add( 1 );
                                progressPart = 0;
                            }
                        }
                    }
                    localCacheProgress.done();
                };
            }

            // Run the workers that cache the index contents and that do partition-local uniqueness checking, if index is unique
            context.execution.run( "Cache index", workers );

            // Then, also if the index is unique then do uniqueness checking of the seams between the partitions
            if ( index.descriptor.isUnique() && !context.isCancelled() )
            {
                for ( int i = 0; i < partitions.length - 1; i++ )
                {
                    Value[] left = lastValues[i];
                    Value[] right = firstValues[i + 1];
                    if ( Arrays.equals( left, right ) )
                    {
                        long leftEntityId = lastEntityIds[i];
                        long rightEntityId = firstEntityIds[i + 1];
                        reporter.forNode( context.recordLoader.node( leftEntityId, cursorTracer ) ).uniqueIndexNotUnique( index.descriptor, left,
                                rightEntityId );
                    }
                }
            }
        }
        finally
        {
            IOUtils.closeAll( partitions );
        }
    }

    private void checkVsEntities( List<IndexContext> indexes, LongRange nodeIdRange ) throws Exception
    {
        ParallelExecution execution = context.execution;
        execution.run( getClass().getSimpleName() + "-checkVsEntities",
                execution.partition( nodeIdRange, ( from, to, last ) -> () -> checkVsEntities( indexes, from, to ) ) );
    }

    private void checkVsEntities( List<IndexContext> indexes, long fromEntityId, long toEntityId )
    {
        // This is one thread
        CheckerContext noReportingContext = context.withoutReporting();
        try ( RecordStorageReader reader = new RecordStorageReader( context.neoStores );
              var cursorTracer = context.pageCacheTracer.createPageCursorTracer( CONSISTENCY_INDEX_ENTITY_CHECK_TAG );
              RecordNodeCursor nodeCursor = reader.allocateNodeCursor( cursorTracer );
              RecordReader<DynamicRecord> labelReader = new RecordReader<>( context.neoStores.getNodeStore().getDynamicLabelStore(), cursorTracer );
              SafePropertyChainReader propertyReader = new SafePropertyChainReader( noReportingContext, cursorTracer ) )
        {
            ProgressListener localScanProgress = scanProgress.threadLocalReporter();
            IntObjectHashMap<Value> allValues = new IntObjectHashMap<>();
            var client = cacheAccess.client();
            int numberOfIndexes = indexes.size();
            for ( long entityId = fromEntityId; entityId < toEntityId && !context.isCancelled(); entityId++ )
            {
                nodeCursor.single( entityId );
                if ( nodeCursor.next() )
                {
                    long[] entityTokens = safeGetNodeLabels( noReportingContext, nodeCursor.getId(), nodeCursor.getLabelField(), labelReader, cursorTracer );
                    lightClear( allValues );
                    boolean propertyChainRead =
                            entityTokens != null && propertyReader.read( allValues, nodeCursor, noReportingContext.reporter::forNode, cursorTracer );
                    if ( propertyChainRead )
                    {
                        for ( int i = 0; i < numberOfIndexes; i++ )
                        {
                            IndexContext index = indexes.get( i );
                            IndexDescriptor descriptor = index.descriptor;
                            Value[] values = RecordLoading.entityIntersectionWithSchema( entityTokens, allValues, descriptor.schema() );
                            long cachedValue = client.getFromCache( entityId, i );
                            boolean nodeIsInIndex = (cachedValue & IN_USE_MASK ) != 0;
                            if ( values != null )
                            {
                                // This node should really be in the index, is it?
                                if ( !nodeIsInIndex )
                                {
                                    // It wasn't, report it
                                    getReporter( nodeCursor ).notIndexed( descriptor, Values.asObjects( values ) );
                                }
                                else if ( index.hasValues )
                                {
                                    int cachedChecksum = (int) cachedValue & CHECKSUM_MASK;
                                    int actualChecksum = checksum( values );
                                    if ( cachedChecksum != actualChecksum )
                                    {
                                        getReporter( nodeCursor ).notIndexed( descriptor, Values.asObjects( values ) );
                                    }
                                }
                            }
                            else
                            {
                                if ( nodeIsInIndex )
                                {
                                    reporter.forIndexEntry( new IndexEntry( descriptor, context.tokenNameLookup, entityId ) ).nodeNotInUse(
                                            context.recordLoader.node( entityId, cursorTracer ) );
                                }
                            }
                        }
                    } // else this would be reported elsewhere
                }
                else
                {
                    // This node shouldn't be in any index
                    for ( int i = 0; i < numberOfIndexes; i++ )
                    {
                        boolean isInIndex = (client.getFromCache( entityId, i ) & IN_USE_MASK) != 0;
                        if ( isInIndex )
                        {
                            reporter.forIndexEntry( new IndexEntry( indexes.get( i ).descriptor, context.tokenNameLookup, entityId ) ).nodeNotInUse(
                                    context.recordLoader.node( entityId, cursorTracer ) );
                        }
                    }
                }
                localScanProgress.add( 1 );
            }
            localScanProgress.done();
        }
    }

    @Override
    public String toString()
    {
        return String.format( "%s[entityType:%s,indexesToCheck:%d]", getClass().getSimpleName(), entityType, indexes.size() );
    }

    /**
     * @return a 15-bit checksum of the values.
     */
    private static int checksum( Value[] values )
    {
        int checksum = 0;
        if ( values != null )
        {
            for ( Value value : values )
            {
                checksum ^= value.hashCode();
            }
        }
        int twoByteChecksum = (checksum >>> Short.SIZE) ^ (checksum & 0xFF);
        return twoByteChecksum & CHECKSUM_MASK;
    }

    private ConsistencyReport.PrimitiveConsistencyReport getReporter( PrimitiveRecord cursor )
    {
        if ( EntityType.NODE.equals( entityType ) )
        {
            return reporter.forNode( (NodeRecord) cursor );
        }
        return reporter.forRelationship( (RelationshipRecord) cursor );
    }

    private static class IndexContext
    {
        final IndexDescriptor descriptor;
        final int cacheSlotOffset;
        final boolean hasValues;

        IndexContext( IndexDescriptor descriptor, int cacheSlotOffset )
        {
            this.descriptor = descriptor;
            this.cacheSlotOffset = cacheSlotOffset;
            this.hasValues = IndexSizes.hasValues( descriptor );
        }
    }
}

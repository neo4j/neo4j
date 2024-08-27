/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency.checker;

import static org.neo4j.consistency.checker.RecordLoading.entityIntersectionWithSchema;
import static org.neo4j.consistency.checker.RecordLoading.lightReplace;
import static org.neo4j.values.storable.Values.NO_VALUE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.neo4j.common.EntityType;
import org.neo4j.consistency.checker.ParallelExecution.ThrowingRunnable;
import org.neo4j.consistency.checking.ConsistencyFlags;
import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.synthetic.IndexEntry;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntriesReader;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public abstract class IndexChecker<Record extends PrimitiveRecord> implements Checker {
    private static final String INDEX_CHECKER_TAG = "IndexChecker";
    private static final String CONSISTENCY_INDEX_ENTITY_CHECK_TAG = "consistencyIndexEntityCheck";
    private static final String CONSISTENCY_INDEX_CACHER_TAG = "consistencyIndexCacher";
    private static final int INDEX_CACHING_PROGRESS_FACTOR = 3;
    static final int NUM_INDEXES_IN_CACHE = 5;

    private static final int CHECKSUM_MASK = 0b0111_1111_1111_1111;
    private static final int IN_USE_MASK = 0b1000_0000_0000_0000;
    private static final int CHECKSUM_SIZE = 15;
    private static final int IN_USE_BIT = 1;
    private static final int TOTAL_SIZE = CHECKSUM_SIZE + IN_USE_BIT;

    private final EntityType entityType;
    private final ConsistencyReport.Reporter reporter;
    protected final CheckerContext context;
    private final IndexAccessors indexAccessors;
    private final CacheAccess cacheAccess;
    private final ProgressListener cacheProgress;
    private final ProgressListener scanProgress;
    private final List<IndexDescriptor> indexes;

    IndexChecker(CheckerContext context, EntityType entityType, String entityName) {
        indexAccessors = context.indexAccessors;
        this.context = context;
        this.entityType = entityType;
        this.reporter = context.reporter;
        this.cacheAccess = context.cacheAccess;
        this.indexes = context.indexSizes.largeIndexes(entityType);
        long totalSize = indexes.stream()
                .mapToLong(context.indexSizes::getEstimatedIndexSize)
                .sum();
        int rounds = (indexes.size() - 1) / NUM_INDEXES_IN_CACHE + 1;
        this.scanProgress =
                context.roundInsensitiveProgressReporter(this, entityName + " index checking", rounds * highId());
        // The caching of indexes is generally so much quicker than other things and is based on estimates so dividing
        // by 10
        // makes the progress more even and any estimation flaws less visible.
        this.cacheProgress =
                context.roundInsensitiveProgressReporter(this, entityName + " index caching", totalSize / 3);
    }

    abstract CommonAbstractStore<Record, ?> store();

    abstract long highId();

    abstract Record getEntity(StoreCursors storeCursors, long entityId);

    abstract int[] getEntityTokens(
            CheckerContext context,
            StoreCursors storeCursors,
            Record record,
            RecordReader<DynamicRecord> additionalReader);

    abstract RecordReader<DynamicRecord> additionalEntityTokenReader(CursorContext cursorContext);

    abstract void reportEntityNotInUse(ConsistencyReport.IndexConsistencyReport report, Record record);

    abstract void reportIndexedIncorrectValues(
            ConsistencyReport.IndexConsistencyReport report, Record record, Object[] propertyValues);

    abstract void reportIndexedWhenShouldNot(ConsistencyReport.IndexConsistencyReport report, Record record);

    abstract ConsistencyReport.PrimitiveConsistencyReport getReport(Record cursor, ConsistencyReport.Reporter reporter);

    private ConsistencyReport.PrimitiveConsistencyReport getReport(Record cursor) {
        return getReport(cursor, reporter);
    }

    @Override
    public void check(LongRange entityIdRange, boolean firstRange, boolean lastRange, MemoryTracker memoryTracker)
            throws Exception {
        // While more indexes
        //   Scan through one or more indexes (as sequentially as possible) and cache the entity ids + hash of the
        // indexed value in one bit-set for each index
        //   Then scan through entity store, its entity tokens and relevant properties and hash that value too --> match
        // with the bit-set + hash.

        cacheAccess.setCacheSlotSizesAndClear(
                TOTAL_SIZE, TOTAL_SIZE, TOTAL_SIZE, TOTAL_SIZE, TOTAL_SIZE); // can hold up to 5 indexes
        List<IndexContext> indexesToCheck = new ArrayList<>();
        try (var indexChecker = context.contextFactory.create(INDEX_CHECKER_TAG);
                var storeCursors = new CachedStoreCursors(context.neoStores, indexChecker)) {
            for (int i = 0; i < indexes.size() && !context.isCancelled(); i++) {
                IndexContext index = new IndexContext(indexes.get(i), i % NUM_INDEXES_IN_CACHE);
                indexesToCheck.add(index);
                cacheIndex(index, entityIdRange, firstRange, indexChecker, storeCursors);
                boolean isLastIndex = i == indexes.size() - 1;
                boolean canFitMoreAndIsNotLast = !isLastIndex && index.cacheSlotOffset != NUM_INDEXES_IN_CACHE - 1;
                if (!canFitMoreAndIsNotLast && !context.isCancelled()) {
                    checkVsEntities(indexesToCheck, entityIdRange);
                    indexesToCheck = new ArrayList<>();
                    cacheAccess.clearCache();
                }
            }
        }
    }

    @Override
    public boolean shouldBeChecked(ConsistencyFlags flags) {
        return flags.checkIndexes() && !indexes.isEmpty();
    }

    private void cacheIndex(
            IndexContext index,
            LongRange entityIdRange,
            boolean firstRange,
            CursorContext cursorContext,
            StoreCursors storeCursors)
            throws Exception {
        IndexAccessor accessor = indexAccessors.accessorFor(index.descriptor);
        IndexEntriesReader[] partitions =
                accessor.newAllEntriesValueReader(context.execution.getNumberOfThreads(), cursorContext);
        try {
            Value[][] firstValues = new Value[partitions.length][];
            Value[][] lastValues = new Value[partitions.length][];
            long[] firstEntityIds = new long[partitions.length];
            long[] lastEntityIds = new long[partitions.length];
            ThrowingRunnable[] workers = new ThrowingRunnable[partitions.length];
            for (int i = 0; i < partitions.length; i++) {
                IndexEntriesReader partition = partitions[i];
                int slot = i;
                workers[i] = () -> {
                    int lastChecksum = 0;
                    int progressPart = 0;
                    var client = cacheAccess.client();
                    try (var context = this.context.contextFactory.create(CONSISTENCY_INDEX_CACHER_TAG);
                            var localStoreCursors = new CachedStoreCursors(this.context.neoStores, context);
                            var localCacheProgress = cacheProgress.threadLocalReporter()) {
                        while (partition.hasNext() && !this.context.isCancelled()) {
                            long entityId = partition.next();
                            if (!entityIdRange.isWithinRangeExclusiveTo(entityId)) {
                                if (firstRange && entityId >= highId()) {
                                    reportEntityNotInUse(
                                            reporter.forIndexEntry(new IndexEntry(
                                                    index.descriptor, this.context.tokenNameLookup, entityId)),
                                            getEntity(localStoreCursors, entityId));
                                } else if (firstRange && index.descriptor.isUnique() && index.hasValues) {
                                    // We check all values belonging to unique indexes while we are checking the first
                                    // range, to not
                                    // miss duplicated values belonging to different ranges.
                                    Value[] indexedValues = partition.values();
                                    int checksum = checksum(indexedValues);
                                    assert checksum <= CHECKSUM_MASK;

                                    lastChecksum = verifyUniquenessInPartition(
                                            index,
                                            firstValues,
                                            lastValues,
                                            firstEntityIds,
                                            lastEntityIds,
                                            slot,
                                            lastChecksum,
                                            localStoreCursors,
                                            entityId,
                                            indexedValues,
                                            checksum);
                                }
                                continue;
                            }

                            int data = IN_USE_MASK;
                            if (index.hasValues) {
                                Value[] indexedValues = partition.values();
                                int checksum = checksum(indexedValues);
                                assert checksum <= CHECKSUM_MASK;
                                data |= checksum;

                                // Also take the opportunity to verify uniqueness, if the index is a uniqueness index
                                if (firstRange && index.descriptor.isUnique()) {
                                    lastChecksum = verifyUniquenessInPartition(
                                            index,
                                            firstValues,
                                            lastValues,
                                            firstEntityIds,
                                            lastEntityIds,
                                            slot,
                                            lastChecksum,
                                            localStoreCursors,
                                            entityId,
                                            indexedValues,
                                            checksum);
                                }
                            }
                            client.putToCacheSingle(entityId, index.cacheSlotOffset, data);
                            if (++progressPart == INDEX_CACHING_PROGRESS_FACTOR) {
                                localCacheProgress.add(1);
                                progressPart = 0;
                            }
                        }
                    }
                };
            }

            // Run the workers that cache the index contents and that do partition-local uniqueness checking, if index
            // is unique
            context.execution.run("Cache index", workers);

            // Then, also if the index is unique then do uniqueness checking of the seams between the partitions
            if (firstRange && index.descriptor.isUnique() && !context.isCancelled()) {
                for (int i = 0; i < partitions.length - 1; i++) {
                    Value[] left = lastValues[i];
                    Value[] right = firstValues[i + 1];
                    // Skip any empty partition - can be empty if all entries in a partition of the index were for
                    // entities outside of the current range.
                    if (left != null && right != null && Arrays.equals(left, right)) {
                        long leftEntityId = lastEntityIds[i];
                        long rightEntityId = firstEntityIds[i + 1];
                        getReport(getEntity(storeCursors, leftEntityId))
                                .uniqueIndexNotUnique(index.descriptor, left, rightEntityId);
                    }
                }
            }
        } finally {
            IOUtils.closeAll(partitions);
        }
    }

    private int verifyUniquenessInPartition(
            IndexContext index,
            Value[][] firstValues,
            Value[][] lastValues,
            long[] firstEntityIds,
            long[] lastEntityIds,
            int slot,
            int lastChecksum,
            CachedStoreCursors localStoreCursors,
            long entityId,
            Value[] indexedValues,
            int checksum) {
        if (firstValues[slot] == null) {
            firstValues[slot] = indexedValues;
            firstEntityIds[slot] = entityId;
        }

        if (lastValues[slot] != null && lastChecksum == checksum && Arrays.equals(lastValues[slot], indexedValues)) {
            getReport(getEntity(localStoreCursors, entityId))
                    .uniqueIndexNotUnique(index.descriptor, indexedValues, lastEntityIds[slot]);
        }

        lastValues[slot] = indexedValues;
        lastEntityIds[slot] = entityId;
        return checksum;
    }

    private void checkVsEntities(List<IndexContext> indexes, LongRange entityIdRange) throws Exception {
        ParallelExecution execution = context.execution;
        execution.run(
                getClass().getSimpleName() + "-checkVsEntities",
                execution.partition(entityIdRange, (from, to, last) -> () -> checkVsEntities(indexes, from, to)));
    }

    private void checkVsEntities(List<IndexContext> indexes, long fromEntityId, long toEntityId) {
        // This is one thread
        CheckerContext noReportingContext = context.withoutReporting();
        try (var cursorContext = context.contextFactory.create(CONSISTENCY_INDEX_ENTITY_CHECK_TAG);
                var storeCursors = new CachedStoreCursors(context.neoStores, cursorContext);
                RecordReader<Record> entityReader =
                        new RecordReader<>(store(), true, cursorContext, context.memoryTracker);
                RecordReader<DynamicRecord> entityTokenReader = additionalEntityTokenReader(cursorContext);
                SafePropertyChainReader propertyReader =
                        new SafePropertyChainReader(noReportingContext, cursorContext);
                var localScanProgress = scanProgress.threadLocalReporter()) {
            IntObjectHashMap<Value> allValues = new IntObjectHashMap<>();
            var client = cacheAccess.client();
            int numberOfIndexes = indexes.size();
            for (long entityId = fromEntityId;
                    entityId < toEntityId && !context.isCancelled();
                    entityId++, localScanProgress.add(1)) {
                Record entityRecord = entityReader.read(entityId);
                if (!entityRecord.inUse()) {
                    // This entity shouldn't be in any index
                    for (int i = 0; i < numberOfIndexes; i++) {
                        boolean isInIndex = (client.getFromCache(entityId, i) & IN_USE_MASK) != 0;
                        if (isInIndex) {
                            reportEntityNotInUse(
                                    reporter.forIndexEntry(new IndexEntry(
                                            indexes.get(i).descriptor, context.tokenNameLookup, entityId)),
                                    getEntity(storeCursors, entityId));
                        }
                    }
                    continue;
                }

                int[] entityTokens = getEntityTokens(noReportingContext, storeCursors, entityRecord, entityTokenReader);
                allValues = lightReplace(allValues);
                boolean propertyChainRead = entityTokens != null
                        && propertyReader.read(
                                allValues,
                                entityRecord,
                                record -> getReport(record, noReportingContext.reporter),
                                storeCursors);
                if (!propertyChainRead) {
                    // this would be reported elsewhere
                    continue;
                }

                for (int i = 0; i < numberOfIndexes; i++) {
                    IndexContext index = indexes.get(i);
                    IndexDescriptor descriptor = index.descriptor;
                    long cachedValue = client.getFromCache(entityId, i);
                    boolean entityIsInIndex = (cachedValue & IN_USE_MASK) != 0;
                    Value[] values = entityIntersectionWithSchema(entityTokens, allValues, descriptor);
                    boolean entityShouldBeInIndex = values != null;

                    // edge case for older fulltext
                    if (descriptor.getIndexType() == IndexType.FULLTEXT && !entityShouldBeInIndex && entityIsInIndex) {
                        // Fulltext indexes created before 4.3.0-drop02 can contain empty documents (added when the
                        // schema matched but the values  were not text). The index still works with those  empty
                        // documents present, so we don't want to report them as inconsistencies and force rebuilds.
                        // This utilizes the fact that countIndexedEntities in FulltextIndexReader with non-text
                        // values will ask about documents that doesn't contain those property keys - a document
                        // found by that query should be an empty document we just want to ignore.
                        int[] indexPropertyKeys = descriptor.schema().getPropertyIds();
                        Value[] noValues = new Value[indexPropertyKeys.length];
                        Arrays.fill(noValues, NO_VALUE);
                        long docsWithNoneOfProperties = indexAccessors
                                .readers()
                                .reader(descriptor)
                                .countIndexedEntities(entityId, cursorContext, indexPropertyKeys, noValues);

                        if (docsWithNoneOfProperties != 1) {
                            reportIndexedWhenShouldNot(
                                    reporter.forIndexEntry(
                                            new IndexEntry(descriptor, context.tokenNameLookup, entityId)),
                                    getEntity(storeCursors, entityId));
                        }

                        // usual cases
                    } else if (entityShouldBeInIndex && !entityIsInIndex) {
                        getReport(getEntity(storeCursors, entityId)).notIndexed(descriptor, Values.asObjects(values));

                    } else if (entityShouldBeInIndex && entityIsInIndex && index.hasValues) {
                        int cachedChecksum = (int) cachedValue & CHECKSUM_MASK;
                        int actualChecksum = checksum(values);
                        if (cachedChecksum != actualChecksum) {
                            reportIndexedIncorrectValues(
                                    reporter.forIndexEntry(
                                            new IndexEntry(descriptor, context.tokenNameLookup, entityId)),
                                    getEntity(storeCursors, entityId),
                                    Values.asObjects(values));
                        }

                    } else if (!entityShouldBeInIndex && entityIsInIndex) {
                        reportIndexedWhenShouldNot(
                                reporter.forIndexEntry(new IndexEntry(descriptor, context.tokenNameLookup, entityId)),
                                getEntity(storeCursors, entityId));
                    }
                }
            }
        }
    }

    @Override
    public String toString() {
        return String.format(
                "%s[entityType:%s,indexesToCheck:%d]", getClass().getSimpleName(), entityType, indexes.size());
    }

    /**
     * @return a 15-bit checksum of the values.
     */
    private static int checksum(Value[] values) {
        int checksum = 0;
        if (values != null) {
            // Include ordinal in checksum to be able to find entities with switched order of the values.
            for (int i = 0; i < values.length; i++) {
                checksum ^= values[i].hashCode() * (i + 1);
            }
        }
        int twoByteChecksum = (checksum >>> Short.SIZE) ^ (checksum & 0xFF);
        return twoByteChecksum & CHECKSUM_MASK;
    }

    private static class IndexContext {
        final IndexDescriptor descriptor;
        final int cacheSlotOffset;
        final boolean hasValues;

        IndexContext(IndexDescriptor descriptor, int cacheSlotOffset) {
            this.descriptor = descriptor;
            this.cacheSlotOffset = cacheSlotOffset;
            this.hasValues = IndexSizes.hasValues(descriptor);
        }
    }
}

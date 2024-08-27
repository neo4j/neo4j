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
package org.neo4j.internal.batchimport;

import static org.neo4j.internal.recordstorage.RelationshipCounter.MANUAL_INCREMENTER;
import static org.neo4j.internal.recordstorage.RelationshipCounter.labelsCountsLength;
import static org.neo4j.internal.recordstorage.RelationshipCounter.wildcardCountsLength;
import static org.neo4j.token.api.TokenConstants.ANY_LABEL;
import static org.neo4j.token.api.TokenConstants.ANY_RELATIONSHIP_TYPE;

import org.neo4j.counts.CountsUpdater;
import org.neo4j.internal.batchimport.cache.LongArray;
import org.neo4j.internal.batchimport.cache.NodeLabelsCache;
import org.neo4j.internal.batchimport.cache.NumberArrayFactory;
import org.neo4j.internal.recordstorage.RelationshipCounter;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;

/**
 * Calculates counts as labelId --[type]--> labelId for relationships with the labels coming from its start/end nodes.
 */
public class RelationshipCountsProcessor implements RecordProcessor<RelationshipRecord> {
    private final LongArray labelsCounts;
    private final LongArray wildcardCounts;

    // and grows on demand
    private final CountsUpdater countsUpdater;
    private final long anyLabel;
    private final long anyRelationshipType;
    private final RelationshipCounter counter;

    public RelationshipCountsProcessor(
            NodeLabelsCache nodeLabelCache,
            int highLabelId,
            int highRelationshipTypeId,
            CountsUpdater countsUpdater,
            NumberArrayFactory cacheFactory,
            MemoryTracker memoryTracker) {
        this.countsUpdater = countsUpdater;
        this.anyLabel = highLabelId;
        this.anyRelationshipType = highRelationshipTypeId;
        this.labelsCounts =
                cacheFactory.newLongArray(labelsCountsLength(highLabelId, highRelationshipTypeId), 0, memoryTracker);
        this.wildcardCounts = cacheFactory.newLongArray(wildcardCountsLength(highRelationshipTypeId), 0, memoryTracker);

        NodeLabelsCache.Client nodeLabelsClient = nodeLabelCache.newClient();
        RelationshipCounter.NodeLabelsLookup nodeLabelLookup = nodeId -> nodeLabelCache.get(nodeLabelsClient, nodeId);
        this.counter = new RelationshipCounter(
                nodeLabelLookup, highLabelId, highRelationshipTypeId, wildcardCounts, labelsCounts, MANUAL_INCREMENTER);
    }

    static long calculateMemoryUsage(int highLabelId, int highRelationshipTypeId) {
        long labelsCountsUsage = labelsCountsLength(highLabelId, highRelationshipTypeId) * Long.BYTES;
        long wildcardCountsUsage = wildcardCountsLength(highRelationshipTypeId) * Long.BYTES;
        return labelsCountsUsage + wildcardCountsUsage;
    }

    @Override
    public boolean process(RelationshipRecord record, StoreCursors storeCursors, MemoryTracker memoryTracker) {
        counter.process(record);
        return false;
    }

    @Override
    public void done() {
        for (int wildcardType = 0; wildcardType <= anyRelationshipType; wildcardType++) {
            int type = wildcardType == anyRelationshipType ? ANY_RELATIONSHIP_TYPE : wildcardType;
            long count = wildcardCounts.get(wildcardType);
            countsUpdater.incrementRelationshipCount(ANY_LABEL, type, ANY_LABEL, count);
        }

        for (int labelId = 0; labelId < anyLabel; labelId++) {
            for (int typeId = 0; typeId <= anyRelationshipType; typeId++) {
                long startCount = counter.startLabelCount(labelId, typeId);
                long endCount = counter.endLabelCount(labelId, typeId);
                int type = typeId == anyRelationshipType ? ANY_RELATIONSHIP_TYPE : typeId;

                countsUpdater.incrementRelationshipCount(labelId, type, ANY_LABEL, startCount);
                countsUpdater.incrementRelationshipCount(ANY_LABEL, type, labelId, endCount);
            }
        }
    }

    @Override
    public void mergeResultsFrom(RecordProcessor<RelationshipRecord> other) {
        RelationshipCountsProcessor o = (RelationshipCountsProcessor) other;
        mergeCounts(labelsCounts, o.labelsCounts);
        mergeCounts(wildcardCounts, o.wildcardCounts);
    }

    @Override
    public void close() {
        labelsCounts.close();
        wildcardCounts.close();
    }

    private static void mergeCounts(LongArray destination, LongArray part) {
        long length = destination.length();
        for (long i = 0; i < length; i++) {
            destination.set(i, destination.get(i) + part.get(i));
        }
    }
}

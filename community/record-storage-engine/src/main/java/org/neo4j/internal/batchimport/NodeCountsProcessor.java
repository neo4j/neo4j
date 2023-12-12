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

import static org.neo4j.token.api.TokenConstants.ANY_LABEL;

import org.neo4j.counts.CountsUpdater;
import org.neo4j.internal.batchimport.cache.NodeLabelsCache;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.storageengine.api.cursor.StoreCursors;

/**
 * Calculates counts per label and puts data into {@link NodeLabelsCache} for use by {@link
 * RelationshipCountsProcessor}.
 */
public class NodeCountsProcessor implements RecordProcessor<NodeRecord> {
    private final NodeStore nodeStore;
    private final long[] labelCounts;
    private final ProgressListener progressListener;
    private final NodeLabelsCache cache;
    private final long fromNodeId;
    private final CountsUpdater counts;
    private final int anyLabel;
    private final NodeLabelsCache.Client cacheClient;

    NodeCountsProcessor(
            NodeStore nodeStore,
            NodeLabelsCache cache,
            int highLabelId,
            long fromNodeId,
            CountsUpdater counts,
            ProgressListener progressReporter) {
        this.nodeStore = nodeStore;
        this.cache = cache;
        this.anyLabel = highLabelId;
        this.fromNodeId = fromNodeId;
        this.counts = counts;
        // Instantiate with high id + 1 since we need that extra slot for the ANY count
        this.labelCounts = new long[highLabelId + 1];
        this.progressListener = progressReporter;
        this.cacheClient = cache.newClient();
    }

    @Override
    public boolean process(NodeRecord node, StoreCursors storeCursors) {
        int[] labels = NodeLabelsField.get(node, nodeStore, storeCursors);
        if (labels.length > 0) {
            for (int labelId : labels) {
                if (node.getId() >= fromNodeId) {
                    labelCounts[labelId]++;
                }
            }
            cache.put(cacheClient, node.getId(), labels);
        }
        if (node.getId() >= fromNodeId) {
            labelCounts[anyLabel]++;
        }
        progressListener.add(1);

        // No need to update the store, we're just reading things here
        return false;
    }

    @Override
    public void mergeResultsFrom(RecordProcessor<NodeRecord> other) {
        NodeCountsProcessor o = (NodeCountsProcessor) other;
        for (int i = 0; i < o.labelCounts.length; i++) {
            labelCounts[i] += o.labelCounts[i];
        }
    }

    @Override
    public void done() {
        for (int i = 0; i < labelCounts.length; i++) {
            counts.incrementNodeCount(i == anyLabel ? ANY_LABEL : i, labelCounts[i]);
        }
    }

    @Override
    public void close() {}
}

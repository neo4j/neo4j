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
package org.neo4j.kernel.api.impl.fulltext;

import java.util.ArrayList;
import java.util.List;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.storageengine.api.IndexEntryUpdate;

class EventuallyConsistentIndexUpdater implements IndexUpdater {
    private final DatabaseIndex<? extends IndexReader> index;
    private final IndexUpdater indexUpdater;
    private final IndexUpdateSink indexUpdateSink;
    private final List<IndexEntryUpdate<?>> updates = new ArrayList<>();

    EventuallyConsistentIndexUpdater(
            DatabaseIndex<? extends IndexReader> index, IndexUpdater indexUpdater, IndexUpdateSink indexUpdateSink) {
        this.index = index;
        this.indexUpdater = indexUpdater;
        this.indexUpdateSink = indexUpdateSink;
    }

    @Override
    public void process(IndexEntryUpdate<?> update) {
        updates.add(update);
    }

    @Override
    public void close() {
        if (!updates.isEmpty()) {
            indexUpdateSink.enqueueTransactionBatchOfUpdates(index, indexUpdater, updates);
        }
    }
}

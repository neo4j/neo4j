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
package org.neo4j.internal.counts;

import static org.neo4j.index.internal.gbptree.ValueMerger.MergeResult.REPLACED;
import static org.neo4j.io.IOUtils.closeAllUnchecked;

import org.neo4j.index.internal.gbptree.ValueMerger;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.logging.InternalLogProvider;

/**
 * Writes absolute counts directly into the tree.
 */
class TreeWriter implements CountUpdater.CountWriter {
    private final Writer<CountsKey, CountsValue> treeWriter;
    private final CountsValue value = new CountsValue();
    private final InternalLogProvider userLogProvider;
    private final RememberingReplacingMerger merger = new RememberingReplacingMerger();

    TreeWriter(Writer<CountsKey, CountsValue> treeWriter, InternalLogProvider userLogProvider) {
        this.treeWriter = treeWriter;
        this.userLogProvider = userLogProvider;
    }

    @Override
    public boolean write(CountsKey key, long delta) {
        return merge(treeWriter, key, value.initialize(delta));
    }

    @Override
    public void close() {
        closeAllUnchecked(treeWriter);
    }

    private boolean merge(Writer<CountsKey, CountsValue> writer, CountsKey key, CountsValue value) {
        if (value.count > 0) {
            writer.merge(key, value, merger);
            return merger.oldMergeValue.count == 0;
        } else if (value.count == 0) {
            writer.remove(key);
            return true;
        } else {
            userLogProvider
                    .getLog(this.getClass())
                    .error(
                            "Key '" + key + "' has a negative count.\n"
                                    + "This is a serious error which is typically caused by a store corruption\n"
                                    + "Even thought the database will continue operating, it will do so with reduced functionality\n"
                                    + "The best cause of action is running the consistency checker, fixing the corruption and rebuilding the count store\n"
                                    + "Counts for the problematic key will not be available until the count store is rebuilt.\n");
            writer.merge(key, new CountsValue().initialize(GBPTreeGenericCountsStore.INVALID_COUNT), merger);
        }
        return false;
    }

    private static class RememberingReplacingMerger implements ValueMerger<CountsKey, CountsValue> {
        private final CountsValue oldMergeValue =
                new CountsValue(); // Note: this value is only valid directly after the previous merge

        @Override
        public MergeResult merge(
                CountsKey existingKey, CountsKey newKey, CountsValue existingValue, CountsValue newValue) {
            oldMergeValue.initialize(existingValue.count);
            return REPLACED;
        }
    }
}

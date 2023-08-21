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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.logging.InternalLogProvider;

/**
 * Writes delta counts somewhat directly into the tree. Changes are still gathered in {@link CountsChanges}, but will be written in sorted batches
 * into the tree using a {@link TreeWriter}. This is useful in a batch-insert scenario where changes aren't tied to any transaction and
 * not all absolute counts are known before-hand.
 */
class DeltaTreeWriter implements CountUpdater.CountWriter {
    private final ThrowingSupplier<Writer<CountsKey, CountsValue>, IOException> treeWriter;
    private final Function<CountsKey, AtomicLong> defaultToStoredCount;
    private final Comparator<CountsKey> comparator;
    private final int maxCacheSize;
    private final InternalLogProvider userLogProvider;
    private CountsChanges changes = new MapCountsChanges();
    private int changeCounter;

    DeltaTreeWriter(
            ThrowingSupplier<Writer<CountsKey, CountsValue>, IOException> treeWriter,
            ToLongFunction<CountsKey> lookup,
            Comparator<CountsKey> comparator,
            int maxCacheSize,
            InternalLogProvider userLogProvider) {
        this.treeWriter = treeWriter;
        this.defaultToStoredCount = k -> new AtomicLong(lookup.applyAsLong(k));
        this.comparator = comparator;
        this.maxCacheSize = maxCacheSize;
        this.userLogProvider = userLogProvider;
    }

    @Override
    public boolean write(CountsKey key, long delta) {
        boolean result = changes.add(key, delta, defaultToStoredCount);
        if (++changeCounter == 100) {
            // Don't check size every time, it's unnecessarily expensive
            changeCounter = 0;
            if (changes.size() > maxCacheSize) {
                writeChanges();
            }
        }
        return result;
    }

    private void writeChanges() {
        try (TreeWriter writer = new TreeWriter(treeWriter.get(), userLogProvider)) {
            changes.sortedChanges(comparator)
                    .forEach(entry ->
                            writer.write(entry.getKey(), entry.getValue().get()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        changes = new MapCountsChanges();
    }

    @Override
    public void close() {
        writeChanges();
    }
}

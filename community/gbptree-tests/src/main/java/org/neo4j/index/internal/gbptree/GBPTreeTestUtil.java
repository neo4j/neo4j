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
package org.neo4j.index.internal.gbptree;

import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.pagecache.PageCache;

class GBPTreeTestUtil {
    static <KEY> boolean contains(List<KEY> expectedKeys, KEY key, Comparator<KEY> comparator) {
        return expectedKeys.stream().map(bind(comparator::compare, key)).anyMatch(Predicate.isEqual(0));
    }

    private static <T, U, R> Function<U, R> bind(BiFunction<T, U, R> f, T t) {
        return u -> f.apply(t, u);
    }

    static int calculatePayloadSize(PageCache pageCache, ImmutableSet<OpenOption> openOptions) {
        var reservedBytes = pageCache.pageReservedBytes(openOptions);
        return pageCache.pageSize() - reservedBytes;
    }

    static boolean consistencyCheckStrict(MultiRootGBPTree<?, ?, ?> tree) throws IOException {
        return tree.consistencyCheck(
                new ThrowingConsistencyCheckVisitor(),
                true,
                NULL_CONTEXT_FACTORY,
                Runtime.getRuntime().availableProcessors(),
                ProgressMonitorFactory.NONE);
    }

    static boolean consistencyCheck(MultiRootGBPTree<?, ?, ?> tree, GBPTreeConsistencyCheckVisitor visitor) {
        return consistencyCheck(tree, visitor, Runtime.getRuntime().availableProcessors());
    }

    static boolean consistencyCheck(
            MultiRootGBPTree<?, ?, ?> tree, GBPTreeConsistencyCheckVisitor visitor, int numThreads) {
        return tree.consistencyCheck(visitor, true, NULL_CONTEXT_FACTORY, numThreads, ProgressMonitorFactory.NONE);
    }
}

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
package org.neo4j.kernel.impl.api.index;

import java.util.Collection;
import java.util.function.UnaryOperator;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.values.storable.Value;

public class IndexMapReference implements IndexMapSnapshotProvider {
    private volatile IndexMap indexMap = new IndexMap();

    @Override
    public IndexMap indexMapSnapshot() {
        return new IndexMap(indexMap);
    }

    /**
     * Modifies the index map under synchronization. Accepts a {@link ThrowingFunction} which gets as input
     * a snapshot of the current {@link IndexMap}. That {@link IndexMap} is meant to be modified by the function
     * and in the end returned. The function can also return another {@link IndexMap} instance if it wants to, e.g.
     * for clearing the map. The returned map will be set as the current index map before exiting the method.
     * This is the only way contents of the {@link IndexMap} considered the current one can be modified.
     *
     * @param modifier the function modifying the snapshot.
     */
    public synchronized void modify(UnaryOperator<IndexMap> modifier) {
        IndexMap snapshot = indexMapSnapshot();
        indexMap = modifier.apply(snapshot);
    }

    public IndexProxy getIndexProxy(IndexDescriptor index) throws IndexNotFoundKernelException {
        IndexProxy proxy = indexMap.getIndexProxy(index);
        if (proxy == null) {
            throw new IndexNotFoundKernelException("No index for index " + index + " exists.");
        }
        return proxy;
    }

    Collection<IndexProxy> getAllIndexProxies() {
        return indexMap.getAllIndexProxies();
    }

    IndexUpdaterMap createIndexUpdaterMap(IndexUpdateMode mode, boolean parallel) {
        return new IndexUpdaterMap(indexMap, mode, parallel);
    }

    public void validateBeforeCommit(IndexDescriptor index, Value[] tuple, long entityId) {
        IndexProxy proxy = indexMap.getIndexProxy(index);
        if (proxy != null) {
            // Do this null-check since from the outside there's a best-effort matching going on between updates and
            // actual indexes backing those.
            proxy.validateBeforeCommit(tuple, entityId);
        }
    }
}

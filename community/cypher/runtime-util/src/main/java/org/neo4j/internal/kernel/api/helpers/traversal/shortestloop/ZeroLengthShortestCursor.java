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
package org.neo4j.internal.kernel.api.helpers.traversal.shortestloop;

import static org.neo4j.values.virtual.VirtualValues.pathReference;

import java.util.Iterator;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.helpers.traversal.ShortestPathBFS;
import org.neo4j.values.virtual.PathReference;

/**
 * Basic cursor that only returns startNode.
 */
public final class ZeroLengthShortestCursor extends UndirectedShortestLoopCursor implements ShortestPathBFS {
    private final PathReference pathReference;
    private boolean hasNext;

    public ZeroLengthShortestCursor(long startNode) {
        long[] nodes = new long[1];
        long[] relationships = new long[0];
        nodes[0] = startNode;
        hasNext = true;
        pathReference = pathReference(nodes, relationships);
    }

    @Override
    public boolean next() {
        if (!hasNext) {
            return false;
        }
        hasNext = false;
        return true;
    }

    @Override
    public PathReference path() {
        if (hasNext) {
            return pathReference;
        }
        return null;
    }

    @Override
    public Iterator<PathReference> shortestPathIterator() {
        return Iterators.iterator(pathReference);
    }

    @Override
    public void closeInternal() {}

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void setTracer(KernelReadTracer tracer) {}
}

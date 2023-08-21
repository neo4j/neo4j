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

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.eclipse.collections.api.list.primitive.ImmutableLongList;

public record GBPTreeInspection(
        List<Tree> trees,
        List<FreelistEntry> allFreelistEntries,
        ImmutableLongList unreleasedFreelistEntries,
        TreeState treeState) {
    public Tree single() {
        return trees.get(0);
    }

    public Tree rootTree() {
        return trees.stream().filter(t -> !t.isDataTree).findAny().orElseThrow();
    }

    public Stream<Tree> dataTrees() {
        return trees.stream().filter(t -> t.isDataTree);
    }

    public record Tree(
            ImmutableLongList internalNodes,
            ImmutableLongList leafNodes,
            ImmutableLongList allNodes,
            ImmutableLongList offloadNodes,
            Map<Long, Integer> keyCounts,
            List<ImmutableLongList> nodesPerLevel,
            long rootNode,
            int lastLevel,
            boolean isDataTree) {}
}

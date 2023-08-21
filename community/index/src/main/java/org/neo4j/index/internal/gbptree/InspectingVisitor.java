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

import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.api.list.primitive.ImmutableLongList;
import org.eclipse.collections.api.list.primitive.LongList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.factory.primitive.LongLists;

public class InspectingVisitor<ROOT_KEY, KEY, VALUE> extends GBPTreeVisitor.Adaptor<ROOT_KEY, KEY, VALUE> {
    private final MutableLongList internalNodes = LongLists.mutable.empty();
    private final MutableLongList leafNodes = LongLists.mutable.empty();
    private final MutableLongList allNodes = LongLists.mutable.empty();
    private final MutableLongList offloadNodes = LongLists.mutable.empty();
    private final Map<Long, Integer> allKeyCounts = new HashMap<>();
    private final List<LongList> nodesPerLevel = new ArrayList<>();
    private final List<GBPTreeInspection.Tree> trees = new ArrayList<>();
    private final List<FreelistEntry> allFreelistEntries = new ArrayList<>();
    private final MutableLongList unreleasedFreelistEntries = LongLists.mutable.empty();
    private long rootNode;
    private int lastLevel;
    private TreeState treeState;
    private MutableLongList currentLevelNodes;
    private long currentFreelistPage;

    public InspectingVisitor() {
        clear();
    }

    public GBPTreeInspection get() {
        return new GBPTreeInspection(
                List.copyOf(trees),
                unmodifiableList(allFreelistEntries),
                unreleasedFreelistEntries.toImmutable(),
                treeState);
    }

    @Override
    public void treeState(Pair<TreeState, TreeState> statePair) {
        this.treeState = TreeStatePair.selectNewestValidOrFirst(statePair);
    }

    @Override
    public void beginTree(boolean dataTree) {
        internalNodes.clear();
        leafNodes.clear();
        allNodes.clear();
        offloadNodes.clear();
        allKeyCounts.clear();
        nodesPerLevel.clear();
        clear();
    }

    @Override
    public void beginLevel(int level) {
        lastLevel = level;
        currentLevelNodes = LongLists.mutable.empty();
        nodesPerLevel.add(currentLevelNodes);
    }

    @Override
    public void beginNode(long pageId, boolean isLeaf, long generation, int keyCount) {
        if (lastLevel == 0) {
            if (rootNode != -1) {
                throw new IllegalStateException("Expected to only have a single node on level 0");
            }
            rootNode = pageId;
        }

        currentLevelNodes.add(pageId);
        allNodes.add(pageId);
        allKeyCounts.put(pageId, keyCount);
        if (isLeaf) {
            leafNodes.add(pageId);
        } else {
            internalNodes.add(pageId);
        }
    }

    @Override
    public void endTree(boolean dataTree) {
        final List<ImmutableLongList> immutableNodesPerLevel =
                nodesPerLevel.stream().map(LongLists.immutable::ofAll).collect(Collectors.toList());
        trees.add(new GBPTreeInspection.Tree(
                internalNodes.toImmutable(),
                leafNodes.toImmutable(),
                allNodes.toImmutable(),
                offloadNodes.toImmutable(),
                unmodifiableMap(allKeyCounts),
                immutableNodesPerLevel,
                rootNode,
                lastLevel,
                dataTree));
    }

    @Override
    public void beginFreelistPage(long pageId) {
        currentFreelistPage = pageId;
    }

    @Override
    public void freelistEntry(long pageId, long generation, int pos) {
        allFreelistEntries.add(new FreelistEntry(currentFreelistPage, pos, pageId, generation));
    }

    @Override
    public void key(KEY key, boolean isLeaf, long offloadId) {
        if (offloadId != TreeNodeUtil.NO_OFFLOAD_ID) {
            offloadNodes.add(offloadId);
        }
    }

    private void clear() {
        rootNode = -1;
        lastLevel = -1;
    }
}

/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.index.internal.gbptree;

import org.eclipse.collections.api.list.primitive.ImmutableLongList;

import java.util.List;
import java.util.Map;

public class GBPTreeInspection<KEY,VALUE>
{
    private final ImmutableLongList internalNodes;
    private final ImmutableLongList leafNodes;
    private final ImmutableLongList allNodes;
    private final Map<Long,Integer> keyCounts;
    private final List<ImmutableLongList> nodesPerLevel;
    private final List<InspectingVisitor.FreelistEntry> allFreelistEntries;
    private final long rootNode;
    private final int lastLevel;
    private final TreeState treeState;

    GBPTreeInspection( ImmutableLongList internalNodes, ImmutableLongList leafNodes, ImmutableLongList allNodes, Map<Long,Integer> keyCounts,
            List<ImmutableLongList> nodesPerLevel, List<InspectingVisitor.FreelistEntry> allFreelistEntries, long rootNode, int lastLevel, TreeState treeState )
    {
        this.internalNodes = internalNodes;
        this.leafNodes = leafNodes;
        this.allNodes = allNodes;
        this.keyCounts = keyCounts;
        this.nodesPerLevel = nodesPerLevel;
        this.allFreelistEntries = allFreelistEntries;
        this.rootNode = rootNode;
        this.lastLevel = lastLevel;
        this.treeState = treeState;
    }

    public ImmutableLongList getInternalNodes()
    {
        return internalNodes;
    }

    public ImmutableLongList getLeafNodes()
    {
        return leafNodes;
    }

    public ImmutableLongList getAllNodes()
    {
        return allNodes;
    }

    public Map<Long,Integer> getKeyCounts()
    {
        return keyCounts;
    }

    public List<ImmutableLongList> getNodesPerLevel()
    {
        return nodesPerLevel;
    }

    public List<InspectingVisitor.FreelistEntry> getAllFreelistEntries()
    {
        return allFreelistEntries;
    }

    public long getRootNode()
    {
        return rootNode;
    }

    public int getLastLevel()
    {
        return lastLevel;
    }

    public TreeState getTreeState()
    {
        return treeState;
    }
}

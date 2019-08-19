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

import java.util.List;
import java.util.Map;

public class GBPTreeInspection<KEY,VALUE>
{
    private final List<Long> internalNodes;
    private final List<Long> leafNodes;
    private final List<Long> allNodes;
    private final Map<Long,Integer> keyCounts;
    private final List<List<Long>> nodesPerLevel;
    private final List<InspectingVisitor.FreelistEntry> allFreelistEntries;
    private final long rootNode;
    private final int lastLevel;
    private final TreeState treeState;

    GBPTreeInspection( List<Long> internalNodes, List<Long> leafNodes, List<Long> allNodes, Map<Long,Integer> keyCounts, List<List<Long>> nodesPerLevel,
            List<InspectingVisitor.FreelistEntry> allFreelistEntries, long rootNode, int lastLevel,
            TreeState treeState )
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

    public List<Long> getInternalNodes()
    {
        return internalNodes;
    }

    public List<Long> getLeafNodes()
    {
        return leafNodes;
    }

    public List<Long> getAllNodes()
    {
        return allNodes;
    }

    public Map<Long,Integer> getKeyCounts()
    {
        return keyCounts;
    }

    public List<List<Long>> getNodesPerLevel()
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

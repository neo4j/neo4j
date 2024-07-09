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
package org.neo4j.internal.batchimport;

import java.util.Random;
import org.neo4j.batchimport.api.input.Group;
import org.neo4j.internal.batchimport.input.Groups;

/**
 * A little utility for randomizing dividing up nodes into {@link Group id spaces}.
 * Supplied with number of nodes to divide up and number of groups
 * to divide into, the group sizes are randomized and together they will contain all nodes.
 */
public class IdGroupDistribution {
    private final long[] groupCounts;
    private final Groups groups;

    public IdGroupDistribution(long nodeCount, int numberOfGroups, Random random, Groups groups) {
        this.groups = groups;
        groupCounts = new long[numberOfGroups];

        // Assign all except the last one
        long total = 0;
        long partSize = nodeCount / numberOfGroups;
        float debt = 1f;
        for (int i = 0; i < numberOfGroups - 1; i++) {
            float part = random.nextFloat() * debt;
            assignGroup(i, (long) (partSize * part));
            total += groupCounts[i];
            debt = debt + 1.0f - part;
        }

        // Assign the rest to the last one
        assignGroup(numberOfGroups - 1, nodeCount - total);
    }

    private void assignGroup(int i, long count) {
        groupCounts[i] = count;
        groups.getOrCreate("Group" + i);
    }

    public Group groupOf(long nodeInOrder) {
        long at = 0;
        for (int i = 0; i < groupCounts.length; i++) {
            at += groupCounts[i];
            if (nodeInOrder < at) {
                return groups.get(i);
            }
        }
        throw new IllegalArgumentException("Strange, couldn't find group for node (import order) " + nodeInOrder
                + ", counted to " + at + " as total number of " + at);
    }
}

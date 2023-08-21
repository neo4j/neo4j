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
package org.neo4j.graphdb.traversal;

import static org.neo4j.graphdb.traversal.Paths.singleNodePath;

import java.util.Comparator;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.internal.helpers.collection.Iterables;

/**
 * Provides some common traversal sorting, used by
 * {@link TraversalDescription#sort(Comparator)}.
 */
public abstract class Sorting {
    // No instances
    private Sorting() {}

    /**
     * Sorts {@link Path}s by the property value of each path's end node.
     *
     * @param propertyKey the property key of the values to sort on.
     * @return a {@link Comparator} suitable for sorting traversal results.
     */
    public static Comparator<? super Path> endNodeProperty(final String propertyKey) {
        return new EndNodeComparator() {
            @SuppressWarnings({"rawtypes", "unchecked"})
            @Override
            protected int compareNodes(Node endNode1, Node endNode2) {
                Comparable p1 = (Comparable) endNode1.getProperty(propertyKey);
                Comparable p2 = (Comparable) endNode2.getProperty(propertyKey);
                if (p1 == p2) {
                    return 0;
                } else if (p1 == null) {
                    return Integer.MIN_VALUE;
                } else if (p2 == null) {
                    return Integer.MAX_VALUE;
                } else {
                    return p1.compareTo(p2);
                }
            }
        };
    }

    /**
     * Sorts {@link Path}s by the relationship count returned for its end node
     * by the supplied {@code expander}.
     *
     * @param expander the {@link PathExpander} to use for getting relationships
     * off of each {@link Path}'s end node.
     * @return a {@link Comparator} suitable for sorting traversal results.
     */
    public static Comparator<? super Path> endNodeRelationshipCount(final PathExpander expander) {
        return new EndNodeComparator() {
            @Override
            protected int compareNodes(Node endNode1, Node endNode2) {
                final var count1 = count(endNode1, expander);
                final var count2 = count(endNode2, expander);
                return Long.compare(count1, count2);
            }

            private long count(Node node, PathExpander expander) {
                return Iterables.count(expander.expand(singleNodePath(node), BranchState.NO_STATE));
            }
        };
    }

    /**
     * Comparator for {@link Path#endNode() end nodes} of two {@link Path paths}
     */
    private abstract static class EndNodeComparator implements Comparator<Path> {
        @Override
        public int compare(Path p1, Path p2) {
            return compareNodes(p1.endNode(), p2.endNode());
        }

        protected abstract int compareNodes(Node endNode1, Node endNode2);
    }
}

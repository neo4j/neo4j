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

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.graphdb.PathExpander;

/**
 * A catalog of convenient branch ordering policies.
 */
@PublicApi
public enum BranchOrderingPolicies implements BranchOrderingPolicy {
    /**
     * This `BranchOrderingPolicy` traverses depth first, visiting the current node, then recursively traversing
     * depth first the current nodes left subtree, before the right subtree. The pre-order traversal is topologically sorted
     * as parent nodes are processed before any of its child nodes are done.
     */
    PREORDER_DEPTH_FIRST {
        @Override
        public BranchSelector create(TraversalBranch startSource, PathExpander expander) {
            return new PreorderDepthFirstSelector(startSource, expander);
        }
    },

    /**
     * This `BranchOrderingPolicy` traverses depth first, recursively traversing down the current nodes left subtree,
     * then the right subtree before visiting the current node.
     */
    POSTORDER_DEPTH_FIRST {
        @Override
        public BranchSelector create(TraversalBranch startSource, PathExpander expander) {
            return new PostorderDepthFirstSelector(startSource, expander);
        }
    },

    /**
     * This `BranchOrderingPolicy` traverses breadth first, visiting first the current node, then each of its children,
     * before continuing to their children and so forth. Providing a level order search.
     */
    PREORDER_BREADTH_FIRST {
        @Override
        public BranchSelector create(TraversalBranch startSource, PathExpander expander) {
            return new PreorderBreadthFirstSelector(startSource, expander);
        }
    },

    /**
     * This `BranchOrderingPolicy` traverses breadth first, visiting all the leaf nodes of the current node before
     * visiting their parents. Effectively searching nodes in a reversed level order search to {@link PREORDER_BREADTH_FIRST}
     */
    POSTORDER_BREADTH_FIRST {
        @Override
        public BranchSelector create(TraversalBranch startSource, PathExpander expander) {
            return new PostorderBreadthFirstSelector(startSource, expander);
        }
    }
}

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

/**
 * A catalogue of convenient side selector policies for use in bidirectional traversals.
 */
@PublicApi
public enum SideSelectorPolicies implements SideSelectorPolicy {
    /**
     * This `SideSelectorPolicy` stops traversal if the combined depth is larger than the given maximum depth. It
     * will select branches for expansion that are on the same depth as the current branch before moving on to the
     * next depth.
     */
    LEVEL {
        @Override
        public SideSelector create(BranchSelector start, BranchSelector end, int maxDepth) {
            return new LevelSelectorOrderer(start, end, false, maxDepth);
        }
    },
    /**
     * This `SideSelectorPolicy` stops as soon as a result is found. It will select branches for expansion that are on
     * the same depth as the current branch before moving on to the next depth.
     */
    LEVEL_STOP_DESCENT_ON_RESULT {
        @Override
        public SideSelector create(BranchSelector start, BranchSelector end, int maxDepth) {
            return new LevelSelectorOrderer(start, end, true, maxDepth);
        }
    },
    /**
     * This `SideSelectorPolicy` alternates which branch continues the traversal.
     */
    ALTERNATING {
        @Override
        public SideSelector create(BranchSelector start, BranchSelector end, int maxDepth) {
            return new AlternatingSelectorOrderer(start, end);
        }
    }
}

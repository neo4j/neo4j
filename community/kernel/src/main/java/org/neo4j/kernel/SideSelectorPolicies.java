/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel;

import org.neo4j.graphdb.traversal.BranchSelector;
import org.neo4j.graphdb.traversal.SideSelector;
import org.neo4j.graphdb.traversal.SideSelectorPolicy;

/**
 * @deprecated See {@link org.neo4j.graphdb.traversal.SideSelectorPolicies}
 */
public enum SideSelectorPolicies implements SideSelectorPolicy
{
    /**
     * @deprecated See {@link org.neo4j.graphdb.traversal.SideSelectorPolicies}
     */
    LEVEL
    {
        @Override
        public SideSelector create( BranchSelector start, BranchSelector end, int maxDepth )
        {
            return org.neo4j.graphdb.traversal.SideSelectorPolicies.LEVEL.create( start, end, maxDepth );
        }
    },

    /**
     * @deprecated See {@link org.neo4j.graphdb.traversal.SideSelectorPolicies}
     */
    LEVEL_STOP_DESCENT_ON_RESULT
    {
        @Override
        public SideSelector create( BranchSelector start, BranchSelector end, int maxDepth )
        {
            return org.neo4j.graphdb.traversal.SideSelectorPolicies.LEVEL_STOP_DESCENT_ON_RESULT
                    .create( start, end, maxDepth );
        }
    },

    /**
     * @deprecated See {@link org.neo4j.graphdb.traversal.SideSelectorPolicies}
     */
    ALTERNATING
    {
        @Override
        public SideSelector create( BranchSelector start, BranchSelector end, int maxDepth )
        {
            return org.neo4j.graphdb.traversal.SideSelectorPolicies.ALTERNATING.create( start, end, maxDepth );
        }
    };
}

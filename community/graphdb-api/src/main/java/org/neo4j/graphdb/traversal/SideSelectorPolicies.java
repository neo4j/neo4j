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
package org.neo4j.graphdb.traversal;

/**
 * A catalogue of convenient side selector policies for use in bidirectional traversals.
 *
 * Copied from kernel package so that we can hide kernel from the public API.
 */
public enum SideSelectorPolicies implements SideSelectorPolicy
{
    LEVEL
    {
        @Override
        public SideSelector create( BranchSelector start, BranchSelector end, int maxDepth )
        {
            return new LevelSelectorOrderer( start, end, false, maxDepth );
        }
    },
    LEVEL_STOP_DESCENT_ON_RESULT
    {
        @Override
        public SideSelector create( BranchSelector start, BranchSelector end, int maxDepth )
        {
            return new LevelSelectorOrderer( start, end, true, maxDepth );
        }
    },
    ALTERNATING
    {
        @Override
        public SideSelector create( BranchSelector start, BranchSelector end, int maxDepth )
        {
            return new AlternatingSelectorOrderer( start, end );
        }
    }
}

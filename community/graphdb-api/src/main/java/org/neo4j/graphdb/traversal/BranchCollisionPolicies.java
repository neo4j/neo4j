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

import java.util.function.Predicate;
import org.neo4j.annotations.api.PublicApi;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.impl.traversal.ShortestPathsBranchCollisionDetector;
import org.neo4j.graphdb.impl.traversal.StandardBranchCollisionDetector;

/**
 * A catalogue of convenient branch collision policies, see {@link BranchCollisionPolicy}
 */
@PublicApi
public enum BranchCollisionPolicies implements BranchCollisionPolicy {
    /**
     * This branch collision policy includes all combined paths where the traversers collide, which means that the end node of the startSide and endSide
     * traverser paths is identical and filters the resulting paths by applying the evaluator and path predicate.
     */
    STANDARD {
        @Override
        public BranchCollisionDetector create(Evaluator evaluator, Predicate<Path> pathPredicate) {
            return new StandardBranchCollisionDetector(evaluator, pathPredicate);
        }
    },
    /**
     * This branch collision policy includes only the shortest paths where the traversers collide, which means that the end node of the
     * startSide and endSide traverser paths is identical and filters the resulting paths by applying the evaluator and path predicate.
     */
    SHORTEST_PATH {
        @Override
        public BranchCollisionDetector create(Evaluator evaluator, Predicate<Path> pathPredicate) {
            return new ShortestPathsBranchCollisionDetector(evaluator, pathPredicate);
        }
    }
}

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

/**
 * A `BranchCollisionPolicy` defines when a collision is detected and accepted in a bidirectional traversal, see {@link BidirectionalTraversalDescription}.
 *
 * Given an evaluator and a path predicate, a `BranchCollisionPolicy` will create a `BranchCollisionDetector`, which will detect collisions between two
 * traversers and use the `Evaluator` and `Path` predicate to decide whether the resulting path will be included in the result.
 */
@PublicApi
public interface BranchCollisionPolicy {
    BranchCollisionDetector create(Evaluator evaluator, Predicate<Path> pathPredicate);
}

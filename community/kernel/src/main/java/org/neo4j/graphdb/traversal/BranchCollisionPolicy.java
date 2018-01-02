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
package org.neo4j.graphdb.traversal;

import org.neo4j.function.Predicate;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.Path;

/**
 * Copied from kernel package so that we can hide kernel from the public API.
 */
public interface BranchCollisionPolicy
{
    @Deprecated
    BranchCollisionDetector create( Evaluator evaluator );

    BranchCollisionDetector create( Evaluator evaluator, Predicate<Path> pathPredicate );

    abstract class CollisionPolicyAdapter implements BranchCollisionPolicy
    {
        @Override
        public BranchCollisionDetector create( Evaluator evaluator )
        {
            return create( evaluator, Predicates.<Path>alwaysTrue() );
        }
    }

    class CollisionPolicyWrapper implements BranchCollisionPolicy
    {
        private BranchCollisionPolicy policy;

        public CollisionPolicyWrapper( BranchCollisionPolicy policy )
        {
            this.policy = policy;
        }

        @Override
        public BranchCollisionDetector create( Evaluator evaluator )
        {
            return policy.create( evaluator );
        }

        public BranchCollisionDetector create( Evaluator evaluator, Predicate<Path> pathPredicate )
        {
            return create( evaluator );
        }
    }
}

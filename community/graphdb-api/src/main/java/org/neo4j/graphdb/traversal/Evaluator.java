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

import org.neo4j.graphdb.Path;

/**
 * An Evaluator controls what's to be returned from a traversal and also how
 * pruning is done. It looks at a {@link Path} and decides whether or not it
 * should be included in the traversal result. It also decides whether the traverser
 * should continue down that path or if it should be pruned so that the traverser
 * won't continue down that path.
 * 
 * @author Mattias Persson
 * @see Evaluation
 * @see Evaluators
 * @see TraversalDescription#evaluator(Evaluator)
 */
public interface Evaluator
{
    /**
     * Evaluates a {@link Path} and returns an {@link Evaluation} containing
     * information about whether or not to include it in the traversal result,
     * i.e return it from the {@link Traverser}. And also whether or not to
     * continue traversing down that {@code path} or if it instead should be
     * pruned so that the traverser won't continue down that branch represented
     * by {@code path}.
     * 
     * @param path the {@link Path} to evaluate.
     * @return an {@link Evaluation} containing information about whether or not
     * to return it from the {@link Traverser} and whether or not to continue
     * down that path.
     */
    Evaluation evaluate( Path path );

    /**
     * Exposes an {@link Evaluator} as a {@link PathEvaluator}.
     * @param <STATE> the type of state passed into the evaluator.
     */
    class AsPathEvaluator<STATE> implements PathEvaluator<STATE>
    {
        private final Evaluator evaluator;

        public AsPathEvaluator( Evaluator evaluator )
        {
            this.evaluator = evaluator;
        }

        @Override
        public Evaluation evaluate( Path path, BranchState<STATE> state )
        {
            return evaluator.evaluate( path );
        }

        @Override
        public Evaluation evaluate( Path path )
        {
            return evaluator.evaluate( path );
        }
    }
}

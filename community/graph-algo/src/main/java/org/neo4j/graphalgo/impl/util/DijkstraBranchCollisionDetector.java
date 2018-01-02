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
package org.neo4j.graphalgo.impl.util;

import org.neo4j.function.Predicate;
import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.kernel.StandardBranchCollisionDetector;
import org.neo4j.kernel.impl.util.MutableDouble;
import org.neo4j.kernel.impl.util.NoneStrictMath;

/**
 * @author Anton Persson
 */
public class DijkstraBranchCollisionDetector extends StandardBranchCollisionDetector
{
    private final CostEvaluator costEvaluator;
    private final MutableDouble shortestSoFar;
    private final double epsilon;

    public DijkstraBranchCollisionDetector( Evaluator evaluator,
            CostEvaluator costEvaluator,
            MutableDouble shortestSoFar, double epsilon, Predicate<Path> pathPredicate )
    {
        super( evaluator, pathPredicate );
        this.costEvaluator = costEvaluator;
        this.shortestSoFar = shortestSoFar;
        this.epsilon = epsilon;
    }

    @Override
    protected boolean includePath( Path path, TraversalBranch startBranch, TraversalBranch endBranch )
    {
        if ( !super.includePath( path, startBranch, endBranch ) )
        {
            return false;
        }

        /*
        In most cases we could prune startBranch and endBranch here.

        Problem when assuming startBranch and endBranch are pruned:

        Path (s) -...- (c) weight x
        path (s) -...- (a) weight x
        path (d) -...- (y) weight y
        path (b) -...- (y) weight y
        rel (c) - (b) weight z
        rel (a) - (b) weight z
        rel (a) - (d) weight z

              - (c) ----   ---- (d) -
           ...           X           ...
           /  (prune)> /   \ <(prune)  \
         (s) -^v^v- (a) -- (b) -^v^v- (t)

         -^v^v- and ... meaning "some path"

        We expect following collisions:
                1. start (c) - (b) end. Result in path of weight x+z+y
                2. start (a) - (d) end. Result in path of weight x+z+y
                3. start (a) - (b) end. Result in path of weight x+z+y
        However, if branches are pruned on collision 1 and 2. Collision 3 will never happen and thus
        a path is missed.
        */

        double cost = new WeightedPathImpl( costEvaluator, path ).weight();

        if ( cost < shortestSoFar.value )
        {
            shortestSoFar.value = cost;
        }
        if ( NoneStrictMath.compare( cost, shortestSoFar.value, epsilon ) <= 0 )
        {
            return true;
        }

        return false;
    }
}

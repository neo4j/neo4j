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
package org.neo4j.graphalgo.impl.path;

import org.apache.commons.lang3.mutable.MutableDouble;

import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphalgo.impl.util.DijkstraBranchCollisionDetector;
import org.neo4j.graphalgo.impl.util.DijkstraSelectorFactory;
import org.neo4j.graphalgo.impl.util.PathInterest;
import org.neo4j.graphalgo.impl.util.PathInterestFactory;
import org.neo4j.graphalgo.impl.util.TopFetchingWeightedPathIterator;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.BidirectionalTraversalDescription;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.InitialBranchState;
import org.neo4j.graphdb.traversal.PathEvaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.TraversalMetadata;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.util.NoneStrictMath;

import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.helpers.collection.Iterators.firstOrNull;

/**
 * Find (one or all) simple shortest path(s) between two nodes.
 * Shortest referring to least cost evaluated by provided {@link CostEvaluator}.
 * It starts a traversal from both ends and terminates when path(s) has been found.
 *
 * Relationships are traversed in the specified directions from the start node,
 * but in the reverse direction ( {@link org.neo4j.graphdb.Direction#reverse()} ) from the
 * end node. This doesn't affect {@link org.neo4j.graphdb.Direction#BOTH}.
 *
 * @author Anton Persson
 */
public class DijkstraBidirectional implements PathFinder<WeightedPath>
{
    private final PathExpander expander;
    private final InitialBranchState stateFactory;
    private final CostEvaluator<Double> costEvaluator;
    private final double epsilon;
    private Traverser lastTraverser;

    /**
     * See {@link #DijkstraBidirectional(PathExpander, CostEvaluator, double)}
     * Using {@link NoneStrictMath#EPSILON} as tolerance.
     */
    public DijkstraBidirectional( PathExpander expander, CostEvaluator<Double> costEvaluator )
    {
        this( expander, costEvaluator, NoneStrictMath.EPSILON );
    }

    /**
     * Construct a new bidirectional dijkstra algorithm.
     * @param expander          The {@link PathExpander} to be used to decide which relationships
     *                          to expand for each node
     * @param costEvaluator     The {@link CostEvaluator} to be used for calculating cost of a
     *                          relationship
     * @param epsilon           The tolerance level to be used when comparing floating point numbers.
     */
    public DijkstraBidirectional( PathExpander expander, CostEvaluator<Double> costEvaluator, double epsilon )
    {
        this.expander = expander;
        this.costEvaluator = costEvaluator;
        this.epsilon = epsilon;
        this.stateFactory = InitialBranchState.DOUBLE_ZERO;
    }

    @Override
    public Iterable<WeightedPath> findAllPaths( Node start, final Node end )
    {
        final Traverser traverser = traverser( start, end, PathInterestFactory.allShortest( epsilon ) );
        return () -> new TopFetchingWeightedPathIterator( traverser.iterator(), costEvaluator );
    }

    private Traverser traverser( Node start, final Node end, PathInterest interest )
    {
        final MutableDouble shortestSoFar = new MutableDouble( Double.MAX_VALUE );
        final MutableDouble startSideShortest = new MutableDouble( 0 );
        final MutableDouble endSideShortest = new MutableDouble( 0 );
        PathExpander dijkstraExpander = new DijkstraBidirectionalPathExpander( expander, shortestSoFar, true,
                startSideShortest, endSideShortest, epsilon );

        GraphDatabaseService db = start.getGraphDatabase();

        TraversalDescription side = db.traversalDescription().expand( dijkstraExpander, stateFactory )
                .order( new DijkstraSelectorFactory( interest, costEvaluator ) )
                .evaluator( new DijkstraBidirectionalEvaluator( costEvaluator ) )
                .uniqueness( Uniqueness.NODE_PATH );

        TraversalDescription startSide = side;
        TraversalDescription endSide = side.reverse();

        BidirectionalTraversalDescription traversal = db.bidirectionalTraversalDescription()
                .startSide( startSide )
                .endSide( endSide )
                .collisionEvaluator( Evaluators.all() )
                .collisionPolicy( ( evaluator, pathPredicate ) ->
                        new DijkstraBranchCollisionDetector( evaluator, costEvaluator, shortestSoFar, epsilon,
                                pathPredicate ) );

        lastTraverser = traversal.traverse( start, end );
        return lastTraverser;
    }

    @Override
    public WeightedPath findSinglePath( Node start, Node end )
    {
        return firstOrNull( new TopFetchingWeightedPathIterator(
                traverser( start, end, PathInterestFactory.single( epsilon ) ).iterator(), costEvaluator ) );
    }

    @Override
    public TraversalMetadata metadata()
    {
        return lastTraverser.metadata();
    }

    private static class DijkstraBidirectionalPathExpander implements PathExpander<Double>
    {
        private final PathExpander source;
        private final MutableDouble shortestSoFar;
        private final MutableDouble otherSideShortest;
        private final double epsilon;
        private final MutableDouble thisSideShortest;
        private final boolean stopAfterLowestCost;

        DijkstraBidirectionalPathExpander( PathExpander source, MutableDouble shortestSoFar,
                boolean stopAfterLowestCost, MutableDouble thisSideShortest, MutableDouble otherSideShortest,
                double epsilon )
        {
            this.source = source;
            this.shortestSoFar = shortestSoFar;
            this.stopAfterLowestCost = stopAfterLowestCost;
            this.thisSideShortest = thisSideShortest;
            this.otherSideShortest = otherSideShortest;
            this.epsilon = epsilon;
        }

        @Override
        public Iterable<Relationship> expand( Path path, BranchState<Double> state )
        {
            double thisState = state.getState();
            thisSideShortest.setValue( thisState );
            if ( NoneStrictMath.compare( thisState + otherSideShortest.doubleValue(), shortestSoFar.doubleValue(), epsilon ) > 0 &&
                 stopAfterLowestCost )
            {
                return Iterables.emptyResourceIterable();
            }
            return source.expand( path, state );
        }

        @Override
        public PathExpander<Double> reverse()
        {
            return new DijkstraBidirectionalPathExpander( source.reverse(), shortestSoFar, stopAfterLowestCost,
                    otherSideShortest, thisSideShortest, epsilon );
        }
    }

    private static class DijkstraBidirectionalEvaluator extends PathEvaluator.Adapter<Double>
    {
        private final CostEvaluator<Double> costEvaluator;

        DijkstraBidirectionalEvaluator( CostEvaluator<Double> costEvaluator )
        {
            this.costEvaluator = costEvaluator;
        }

        @Override
        public Evaluation evaluate( Path path, BranchState<Double> state )
        {
            double nextState = state.getState();
            if ( path.length() > 0 )
            {
                nextState += costEvaluator.getCost( path.lastRelationship(), OUTGOING );
                state.setState( nextState );
            }
            return Evaluation.EXCLUDE_AND_CONTINUE;
        }
    }
}

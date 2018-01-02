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
package org.neo4j.graphalgo.impl.path;

import java.util.Collections;
import java.util.Iterator;

import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphalgo.impl.util.DijkstraSelectorFactory;
import org.neo4j.graphalgo.impl.util.PathInterest;
import org.neo4j.graphalgo.impl.util.PathInterestFactory;
import org.neo4j.graphalgo.impl.util.WeightedPathIterator;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.InitialBranchState;
import org.neo4j.graphdb.traversal.PathEvaluator;
import org.neo4j.graphdb.traversal.TraversalMetadata;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.kernel.impl.traversal.MonoDirectionalTraversalDescription;
import org.neo4j.kernel.impl.util.MutableDouble;
import org.neo4j.kernel.impl.util.NoneStrictMath;

import static org.neo4j.graphalgo.impl.util.PathInterestFactory.single;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.helpers.collection.IteratorUtil.firstOrNull;
import static org.neo4j.kernel.StandardExpander.toPathExpander;

/**
 * Find (one or some) simple shortest path(s) between two nodes.
 * Shortest referring to least cost evaluated by provided {@link CostEvaluator}.
 *
 * When asking for {@link #findAllPaths(Node, Node)} behaviour will depending on
 * which {@link PathInterest} is used.
 * Recommended option is
 * {@link PathInterestFactory#numberOfShortest(double,int)} - defined number of shortest path in increasing order
 *
 * Also available
 * {@link PathInterestFactory#allShortest(double)}          - Find all paths that are equal in length to shortest.
 *                                                            {@link DijkstraBidirectional} does this faster.
 * {@link PathInterestFactory#all(double)}                  - Find all paths in increasing order. This option has
 *                                                            performance problem and is not recommended.
 *
 * @author Tobias Ivarsson
 * @author Martin Neumann
 * @author Mattias Persson
 * @author Anton Persson
 */
public class Dijkstra implements PathFinder<WeightedPath>
{
    private final PathExpander expander;
    private final InitialBranchState stateFactory;
    private final CostEvaluator<Double> costEvaluator;
    private Traverser lastTraverser;
    private final double epsilon;
    private final PathInterest<Double> interest;
    private final boolean stateInUse;
    // TODO: Remove stateInUse when removing deprecated constructors that uses InitialBranchState.
    // TODO: ALso set traverser to always use DijkstaPathExpander and DijkstraEvaluator.

    /**
     * @deprecated Dijkstra should not be used with state
     * Use {@link #Dijkstra(PathExpander, CostEvaluator)} instead.
     */
    public Dijkstra( PathExpander expander, InitialBranchState stateFactory, CostEvaluator<Double> costEvaluator )
    {
        this( expander, stateFactory, costEvaluator, true );
    }

    /**
     * @deprecated Dijkstra should not be used with state.
     * Use {@link #Dijkstra(PathExpander, CostEvaluator, PathInterest)} instead.
     */
    public Dijkstra( PathExpander expander, InitialBranchState stateFactory, CostEvaluator<Double> costEvaluator,
            boolean stopAfterLowestCost )
    {
        this.expander = expander;
        this.costEvaluator = costEvaluator;
        this.stateFactory = stateFactory;
        interest = stopAfterLowestCost ? PathInterestFactory.allShortest( NoneStrictMath.EPSILON ) :
                                         PathInterestFactory.all( NoneStrictMath.EPSILON );
        epsilon = NoneStrictMath.EPSILON;
        this.stateInUse = true;
    }

    /**
     * See {@link #Dijkstra(PathExpander, CostEvaluator, double, PathInterest)}
     * Use {@link NoneStrictMath#EPSILON} as tolerance.
     * Use {@link PathInterestFactory#allShortest(double)} as PathInterest.
     */
    public Dijkstra( PathExpander expander, CostEvaluator<Double> costEvaluator )
    {
        this( expander, costEvaluator, PathInterestFactory.allShortest( NoneStrictMath.EPSILON ) );
    }

    /**
     * @deprecated in favor for {@link #Dijkstra(PathExpander, CostEvaluator)}
     */
    public Dijkstra( RelationshipExpander expander, CostEvaluator<Double> costEvaluator )
    {
        this( toPathExpander( expander ), costEvaluator );
    }

    /**
     * @deprecated in favor for {@link #Dijkstra(PathExpander, CostEvaluator, PathInterest)}  }.
     */
    public Dijkstra( PathExpander expander, CostEvaluator<Double> costEvaluator,
            boolean stopAfterLowestCost )
    {
        this( expander, costEvaluator, NoneStrictMath.EPSILON, stopAfterLowestCost ?
                                                          PathInterestFactory.allShortest( NoneStrictMath.EPSILON ) :
                                                          PathInterestFactory.all( NoneStrictMath.EPSILON ) );
    }

    /**
     * @deprecated in favor for {@link #Dijkstra( PathExpander, CostEvaluator, PathInterest ) }.
     */
    public Dijkstra( RelationshipExpander expander, CostEvaluator<Double> costEvaluator, boolean stopAfterLowestCost )
    {
        this( toPathExpander( expander ), costEvaluator, stopAfterLowestCost );
    }

    /**
     * See {@link #Dijkstra(PathExpander, CostEvaluator, double, PathInterest)}
     * Use {@link NoneStrictMath#EPSILON} as tolerance.
     */
    public Dijkstra( PathExpander expander, CostEvaluator<Double> costEvaluator, PathInterest<Double> interest )
    {
        this( expander, costEvaluator, NoneStrictMath.EPSILON, interest );
    }

    /**
     * Construct new dijkstra algorithm.
     * @param expander          {@link PathExpander} to be used to decide which relationships
     *                          to expand.
     * @param costEvaluator     {@link CostEvaluator} to be used to calculate cost of relationship
     * @param epsilon           The tolerance level to be used when comparing floating point numbers.
     * @param interest          {@link PathInterest} to be used when deciding if a path is interesting.
     *                          Recommend to use {@link PathInterestFactory} to get reliable behaviour.
     */
    public Dijkstra( PathExpander expander, CostEvaluator<Double> costEvaluator, double epsilon,
            PathInterest<Double> interest )
    {
        this.expander = expander;
        this.costEvaluator = costEvaluator;
        this.epsilon = epsilon;
        this.interest = interest;
        this.stateFactory = InitialBranchState.DOUBLE_ZERO;
        this.stateInUse = false;
    }

    @Override
    public Iterable<WeightedPath> findAllPaths( Node start, final Node end )
    {
        final Traverser traverser = traverser( start, end, interest );
        return new Iterable<WeightedPath>()
        {
            @Override
            public Iterator<WeightedPath> iterator()
            {
                return new WeightedPathIterator( traverser.iterator(), costEvaluator, epsilon,
                        interest );
            }
        };
    }

    private Traverser traverser( Node start, final Node end, PathInterest<Double> interest )
    {
        PathExpander dijkstraExpander;
        PathEvaluator dijkstraEvaluator;
        if ( stateInUse )
        {
            dijkstraExpander = expander;
            dijkstraEvaluator = Evaluators.includeWhereEndNodeIs( end );
        }
        else
        {
            MutableDouble shortestSoFar = new MutableDouble( Double.MAX_VALUE );
            dijkstraExpander = new DijkstraPathExpander( expander, shortestSoFar, epsilon,
                    interest.stopAfterLowestCost() );
            dijkstraEvaluator = new DijkstraEvaluator( shortestSoFar, end, costEvaluator );
        }
        return (lastTraverser = new MonoDirectionalTraversalDescription( )
                .uniqueness( Uniqueness.NODE_PATH )
                .expand( dijkstraExpander, stateFactory )
                .order( new DijkstraSelectorFactory( interest, costEvaluator ) )
                .evaluator( dijkstraEvaluator ).traverse( start ) );
    }

    @Override
    public WeightedPath findSinglePath( Node start, Node end )
    {
        return firstOrNull( new WeightedPathIterator(
                traverser( start, end, single( epsilon ) ).iterator(), costEvaluator, epsilon, interest ) );
    }

    @Override
    public TraversalMetadata metadata()
    {
        return lastTraverser.metadata();
    }

    private static class DijkstraPathExpander implements PathExpander<Double>
    {
        protected final PathExpander source;
        protected MutableDouble shortestSoFar;
        private final double epsilon;
        protected final boolean stopAfterLowestCost;

        DijkstraPathExpander( final PathExpander source,
                MutableDouble shortestSoFar, double epsilon, boolean stopAfterLowestCost )
        {
            this.source = source;
            this.shortestSoFar = shortestSoFar;
            this.epsilon = epsilon;
            this.stopAfterLowestCost = stopAfterLowestCost;
        }

        @Override
        public Iterable<Relationship> expand( Path path, BranchState<Double> state )
        {
            if ( NoneStrictMath.compare( state.getState(), shortestSoFar.value, epsilon ) > 0 && stopAfterLowestCost )
            {
                return Collections.emptyList();
            }
            return source.expand( path, state );
        }

        @Override
        public PathExpander<Double> reverse()
        {
            return new DijkstraPathExpander( source.reverse(), shortestSoFar, epsilon, stopAfterLowestCost );
        }
    }

    private static class DijkstraEvaluator extends PathEvaluator.Adapter<Double>
    {
        private MutableDouble shortestSoFar;
        private Node endNode;
        private final CostEvaluator<Double> costEvaluator;

        DijkstraEvaluator( MutableDouble shortestSoFar, Node endNode, CostEvaluator<Double> costEvaluator )
        {
            this.shortestSoFar = shortestSoFar;
            this.endNode = endNode;
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
            if ( path.endNode().equals( endNode ) )
            {
                shortestSoFar.value = Math.min( shortestSoFar.value, nextState );
                return Evaluation.INCLUDE_AND_PRUNE;
            }
            return Evaluation.EXCLUDE_AND_CONTINUE;
        }
    }

}

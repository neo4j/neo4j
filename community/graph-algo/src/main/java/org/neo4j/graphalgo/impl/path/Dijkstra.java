/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.Iterator;

import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphalgo.impl.util.BestFirstSelectorFactory;
import org.neo4j.graphalgo.impl.util.BestFirstSelectorFactory.PathInterest;
import org.neo4j.graphalgo.impl.util.WeightedPathIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.InitialBranchState;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.TraversalMetadata;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.kernel.Uniqueness;

import static org.neo4j.graphalgo.impl.util.BestFirstSelectorFactory.pathInterest;
import static org.neo4j.helpers.collection.IteratorUtil.firstOrNull;
import static org.neo4j.kernel.StandardExpander.toPathExpander;
import static org.neo4j.kernel.Traversal.traversal;

/**
 * @author Tobias Ivarsson
 * @author Martin Neumann
 * @author Mattias Persson
 */
public class Dijkstra implements PathFinder<WeightedPath>
{
    private static final TraversalDescription TRAVERSAL = traversal().uniqueness( Uniqueness.NONE );

    private final PathExpander expander;
    private final InitialBranchState stateFactory;
    private final CostEvaluator<Double> costEvaluator;
    private Traverser lastTraverser;
    private final boolean stopAfterLowestCost;

    public Dijkstra( PathExpander expander, CostEvaluator<Double> costEvaluator )
    {
        this( expander, InitialBranchState.NO_STATE, costEvaluator, true );
    }

    public Dijkstra( PathExpander expander, InitialBranchState stateFactory, CostEvaluator<Double> costEvaluator )
    {
        this( expander, stateFactory, costEvaluator, true );
    }

    public Dijkstra( RelationshipExpander expander, CostEvaluator<Double> costEvaluator )
    {
        this( toPathExpander( expander ), costEvaluator, true );
    }

    public Dijkstra( PathExpander expander, CostEvaluator<Double> costEvaluator, boolean stopAfterLowestCost )
    {
        this( expander, InitialBranchState.NO_STATE, costEvaluator, stopAfterLowestCost );
    }

    public Dijkstra( PathExpander expander, InitialBranchState stateFactory, CostEvaluator<Double> costEvaluator,
            boolean stopAfterLowestCost )
    {
        this.expander = expander;
        this.costEvaluator = costEvaluator;
        this.stateFactory = stateFactory;
        this.stopAfterLowestCost = stopAfterLowestCost;
    }

    public Dijkstra( RelationshipExpander expander, CostEvaluator<Double> costEvaluator, boolean stopAfterLowestCost )
    {
        this( toPathExpander( expander ), costEvaluator, stopAfterLowestCost );
    }

    @Override
    public Iterable<WeightedPath> findAllPaths( Node start, final Node end )
    {
        final Traverser traverser = traverser( start, end, pathInterest( true, stopAfterLowestCost ) );
        return new Iterable<WeightedPath>()
        {
            @Override
            public Iterator<WeightedPath> iterator()
            {
                return new WeightedPathIterator( traverser.iterator(), costEvaluator, stopAfterLowestCost );
            }
        };
    }

    private Traverser traverser( Node start, final Node end, PathInterest interest )
    {
        return (lastTraverser = TRAVERSAL.expand( expander, stateFactory )
                .order( new SelectorFactory( interest, costEvaluator ) )
                .evaluator( Evaluators.includeWhereEndNodeIs( end ) ).traverse( start ) );
    }

    @Override
    public WeightedPath findSinglePath( Node start, Node end )
    {
        return firstOrNull( new WeightedPathIterator(
                traverser( start, end, PathInterest.singleLowest ).iterator(), costEvaluator, true ) );
    }

    @Override
    public TraversalMetadata metadata()
    {
        return lastTraverser.metadata();
    }

    private static class SelectorFactory extends BestFirstSelectorFactory<Double, Double>
    {
        private final CostEvaluator<Double> evaluator;

        SelectorFactory( PathInterest interest, CostEvaluator<Double> evaluator )
        {
            super( interest );
            this.evaluator = evaluator;
        }

        @Override
        protected Double calculateValue( TraversalBranch next )
        {
            return next.length() == 0 ? 0d : evaluator.getCost(
                    next.lastRelationship(), Direction.OUTGOING );
        }

        @Override
        protected Double addPriority( TraversalBranch source,
                Double currentAggregatedValue, Double value )
        {
            return withDefault( currentAggregatedValue, 0d ) + withDefault( value, 0d );
        }

        private <T> T withDefault( T valueOrNull, T valueIfNull )
        {
            return valueOrNull != null ? valueOrNull : valueIfNull;
        }

        @Override
        protected Double getStartData()
        {
            return 0d;
        }
    }
}

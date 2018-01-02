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

import java.util.Iterator;

import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.EstimateEvaluator;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphalgo.impl.util.BestFirstSelectorFactory;
import org.neo4j.graphalgo.impl.util.PathInterest;
import org.neo4j.graphalgo.impl.util.PathInterestFactory;
import org.neo4j.graphalgo.impl.util.WeightedPathIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.traversal.InitialBranchState;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.TraversalMetadata;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.Uniqueness;

import static org.neo4j.graphdb.traversal.Evaluators.includeWhereEndNodeIs;
import static org.neo4j.graphdb.traversal.InitialBranchState.NO_STATE;
import static org.neo4j.helpers.collection.IteratorUtil.firstOrNull;
import static org.neo4j.kernel.StandardExpander.toPathExpander;
import static org.neo4j.kernel.Traversal.traversal;

/**
 * Implementation of A* algorithm, see {@link AStar}, but using the traversal
 * framework. It's still in an experimental state.
 *
 * @author Mattias Persson
 *
 */
public class TraversalAStar implements PathFinder<WeightedPath>
{
    private final TraversalDescription traversalDescription;
    private final CostEvaluator<Double> costEvaluator;
    private Traverser lastTraverser;

    private final EstimateEvaluator<Double> estimateEvaluator;
    private boolean stopAfterLowestWeight;

    @SuppressWarnings( "unchecked" )
    public <T> TraversalAStar( PathExpander<T> expander,
            CostEvaluator<Double> costEvaluator, EstimateEvaluator<Double> estimateEvaluator )
    {
        this( expander, NO_STATE, costEvaluator, estimateEvaluator, true );
    }

    public <T> TraversalAStar( PathExpander<T> expander, InitialBranchState<T> initialState,
            CostEvaluator<Double> costEvaluator, EstimateEvaluator<Double> estimateEvaluator )
    {
        this( expander, initialState, costEvaluator, estimateEvaluator, true );
    }

    @SuppressWarnings( "unchecked" )
    public TraversalAStar( RelationshipExpander expander, CostEvaluator<Double> costEvaluator,
            EstimateEvaluator<Double> estimateEvaluator )
    {
        this( toPathExpander( expander ), costEvaluator, estimateEvaluator, true );
    }

    @SuppressWarnings( "unchecked" )
    public <T> TraversalAStar( PathExpander<T> expander,
            CostEvaluator<Double> costEvaluator, EstimateEvaluator<Double> estimateEvaluator,
            boolean stopAfterLowestWeight )
    {
        this( expander, NO_STATE, costEvaluator, estimateEvaluator, stopAfterLowestWeight );
    }

    public <T> TraversalAStar( PathExpander<T> expander, InitialBranchState<T> initialState,
            CostEvaluator<Double> costEvaluator, EstimateEvaluator<Double> estimateEvaluator,
            boolean stopAfterLowestWeight )
    {
        this.costEvaluator = costEvaluator;
        this.estimateEvaluator = estimateEvaluator;
        this.stopAfterLowestWeight = stopAfterLowestWeight;
        this.traversalDescription = traversal().uniqueness( Uniqueness.NONE ).expand( expander, initialState );
    }

    @SuppressWarnings( "unchecked" )
    public TraversalAStar( RelationshipExpander expander, CostEvaluator<Double> costEvaluator,
            EstimateEvaluator<Double> estimateEvaluator, boolean stopAfterLowestWeight )
    {
        this( toPathExpander( expander ), costEvaluator, estimateEvaluator, stopAfterLowestWeight );
    }

    @Override
    public Iterable<WeightedPath> findAllPaths( Node start, final Node end )
    {
        return IteratorUtil.asIterable( findSinglePath( start, end ) );
    }

    @Override
    public WeightedPath findSinglePath( Node start, Node end )
    {
        return firstOrNull( findPaths( start, end, false ) );
    }

    private Iterable<WeightedPath> findPaths( Node start, Node end, boolean multiplePaths )
    {
        PathInterest interest;
        if ( multiplePaths )
        {
            interest = stopAfterLowestWeight ? PathInterestFactory.allShortest() : PathInterestFactory.all();
        }
        else {
            interest = PathInterestFactory.single();
        }

        lastTraverser = traversalDescription.order(
                new SelectorFactory( end, interest ) )
                .evaluator( includeWhereEndNodeIs( end ) )
                .traverse( start );
        return new Iterable<WeightedPath>()
        {
            @Override
            public Iterator<WeightedPath> iterator()
            {
                return new WeightedPathIterator( lastTraverser.iterator(), costEvaluator, stopAfterLowestWeight );
            }
        };
    }

    @Override
    public TraversalMetadata metadata()
    {
        return lastTraverser.metadata();
    }

    private static class PositionData implements Comparable<PositionData>
    {
        private final double wayLengthG;
        private final double estimateH;

        public PositionData( double wayLengthG, double estimateH )
        {
            this.wayLengthG = wayLengthG;
            this.estimateH = estimateH;
        }

        Double f()
        {
            return this.estimateH + this.wayLengthG;
        }

        @Override
        public int compareTo( PositionData o )
        {
            return f().compareTo( o.f() );
        }

        @Override
        public String toString()
        {
            return "g:" + wayLengthG + ", h:" + estimateH;
        }
    }

    private class SelectorFactory extends BestFirstSelectorFactory<PositionData, Double>
    {
        private final Node end;

        SelectorFactory( Node end, PathInterest interest )
        {
            super( interest );
            this.end = end;
        }

        @Override
        protected PositionData addPriority( TraversalBranch source,
                PositionData currentAggregatedValue, Double value )
        {
            return new PositionData( currentAggregatedValue.wayLengthG + value,
                    estimateEvaluator.getCost( source.endNode(), end ) );
        }

        @Override
        protected Double calculateValue( TraversalBranch next )
        {
            return next.length() == 0 ? 0d :
                costEvaluator.getCost( next.lastRelationship(), Direction.OUTGOING );
        }

        @Override
        protected PositionData getStartData()
        {
            return new PositionData( 0, 0 );
        }
    }
}

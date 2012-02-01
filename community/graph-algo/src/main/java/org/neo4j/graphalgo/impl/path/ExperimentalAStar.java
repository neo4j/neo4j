/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import org.neo4j.graphalgo.impl.util.StopAfterWeightIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;

public class ExperimentalAStar implements PathFinder<WeightedPath>
{
    private final TraversalDescription traversalDescription;
    private final CostEvaluator<Double> costEvaluator;

    private final EstimateEvaluator<Double> estimateEvaluator;

    public ExperimentalAStar( RelationshipExpander expander, CostEvaluator<Double> costEvaluator,
            EstimateEvaluator<Double> estimateEvaluator )
    {
        this.traversalDescription = Traversal.description().uniqueness(
                Uniqueness.NONE ).expand( expander );
        this.costEvaluator = costEvaluator;
        this.estimateEvaluator = estimateEvaluator;
    }

    public Iterable<WeightedPath> findAllPaths( Node start, final Node end )
    {
        Predicate<Path> filter = new Predicate<Path>()
        {
            public boolean accept( Path position )
            {
                return position.endNode().equals( end );
            }
        };

        final Traverser traverser = traversalDescription.order(
                new SelectorFactory( end ) ).filter( filter ).traverse( start );
        return new Iterable<WeightedPath>()
        {
            public Iterator<WeightedPath> iterator()
            {
                return new StopAfterWeightIterator( traverser.iterator(),
                        costEvaluator );
            }
        };
    }

    public WeightedPath findSinglePath( Node start, Node end )
    {
        Iterator<WeightedPath> paths = findAllPaths( start, end ).iterator();
        return paths.hasNext() ? paths.next() : null;
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

        SelectorFactory( Node end )
        {
            this.end = end;
        }

        @Override
        protected PositionData addPriority( TraversalBranch source,
                PositionData currentAggregatedValue, Double value )
        {
            return new PositionData( currentAggregatedValue.wayLengthG + value,
                    estimateEvaluator.getCost( source.node(), end ) );
        }

        @Override
        protected Double calculateValue( TraversalBranch next )
        {
            return next.depth() == 0 ? 0d :
                costEvaluator.getCost( next.relationship(), Direction.OUTGOING );
        }

        @Override
        protected PositionData getStartData()
        {
            return new PositionData( 0, 0 );
        }
    }
}

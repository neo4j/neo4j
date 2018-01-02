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

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphalgo.impl.util.PriorityMap.Converter;
import org.neo4j.graphalgo.impl.util.PriorityMap.Entry;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.traversal.BranchOrderingPolicy;
import org.neo4j.graphdb.traversal.BranchSelector;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.graphdb.traversal.TraversalContext;

import static org.neo4j.kernel.StandardExpander.toPathExpander;

public abstract class BestFirstSelectorFactory<P extends Comparable<P>, D>
        implements BranchOrderingPolicy
{
    private final PathInterest<P> interest;

    public BestFirstSelectorFactory( PathInterest<P> interest )
    {
        this.interest = interest;
    }

    @Override
    public BranchSelector create( TraversalBranch startSource, PathExpander expander )
    {
        return new BestFirstSelector( startSource, getStartData(), expander );
    }

    public BranchSelector create( TraversalBranch startSource, RelationshipExpander expander )
    {
        return create( startSource, toPathExpander( expander ) );
    }

    protected abstract P getStartData();

    private static class Visit<P extends Comparable<P>> implements Comparable<P>
    {
        private P cost;
        private int visitCount;

        public Visit( P cost )
        {
            this.cost = cost;
        }

        @Override
        public int compareTo( P o )
        {
            return cost.compareTo( o );
        }
    }

    public final class BestFirstSelector implements BranchSelector
    {
        private final PriorityMap<TraversalBranch, Node, P> queue = new PriorityMap( CONVERTER, interest.comparator(),
                interest.stopAfterLowestCost() );
        private TraversalBranch current;
        private P currentAggregatedValue;
        private final PathExpander expander;
        private final Map<Long, Visit<P>> visits = new HashMap<Long, Visit<P>>();

        public BestFirstSelector( TraversalBranch source, P startData, PathExpander expander )
        {
            this.current = source;
            this.currentAggregatedValue = startData;
            this.expander = expander;
        }

        @Override
        public TraversalBranch next( TraversalContext metadata )
        {
            // Exhaust current if not already exhausted
            while ( true )
            {
                TraversalBranch next = current.next( expander, metadata );
                if ( next == null )
                {
                    break;
                }

                long endNodeId = next.endNode().getId();
                Visit<P> stay = visits.get( endNodeId );

                D cost = calculateValue( next );
                P newPriority = addPriority( next, currentAggregatedValue, cost );

                boolean newStay = stay == null;
                if ( newStay )
                {
                    stay = new Visit<>( newPriority );
                    visits.put( endNodeId, stay );
                }
                if ( newStay || !interest.canBeRuledOut( stay.visitCount, newPriority, stay.cost ) )
                {
                    if ( interest.comparator().compare( newPriority, stay.cost ) < 0 )
                    {
                        stay.cost = newPriority;
                    }
                    queue.put( next, newPriority );
                }
            }

            do
            {
                // Pop the top from priorityMap
                Entry<TraversalBranch, P> entry = queue.pop();
                if ( entry != null )
                {
                    current = entry.getEntity();
                    Visit<P> stay = visits.get( current.endNode().getId() );
                    stay.visitCount++;
                    if ( interest.stillInteresting( stay.visitCount ) )
                    {
                        currentAggregatedValue = entry.getPriority();
                        return current;
                    }
                }
                else
                {
                    return null;
                }
            } while ( true );
        }
    }

    protected abstract P addPriority( TraversalBranch source,
            P currentAggregatedValue, D value );

    protected abstract D calculateValue( TraversalBranch next );

    public static final Converter<Node, TraversalBranch> CONVERTER =
            new Converter<Node, TraversalBranch>()
    {
        @Override
        public Node convert( TraversalBranch source )
        {
            return source.endNode();
        }
    };
}

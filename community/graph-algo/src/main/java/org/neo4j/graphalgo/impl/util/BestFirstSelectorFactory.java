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
import org.neo4j.helpers.Function2;
import static org.neo4j.kernel.StandardExpander.toPathExpander;

public abstract class BestFirstSelectorFactory<P extends Comparable<P>, D>
        implements BranchOrderingPolicy
{
    private final PathInterest interest;
    private final Function2<Visit<P>, P, Boolean> interestPredicate;

    public static enum PathInterest
    {   // Ordered by how expensive they are, ASC

        /* If we're only interested in a single path then we can skip branches that have the same cost
         * to any given end node and only keep one of the lowest cost branches. */
        singleLowest
        {
            @Override
            protected <P extends Comparable<P>> Function2<Visit<P>, P, Boolean> interestFunction()
            {
                return new Function2<Visit<P>,P,Boolean>()
                {
                    @Override
                    public Boolean apply( Visit<P> from1, P from2 )
                    {
                        return from1.compareTo( from2 ) > 0;
                    }
                };
            }
        },

        /* If we are interested in multiple paths then we must keep around branches that have the same
         * cost to any same end node. */
        multipleLowest
        {
            @Override
            protected <P extends Comparable<P>> Function2<Visit<P>, P, Boolean> interestFunction()
            {
                return new Function2<Visit<P>,P,Boolean>()
                {
                    @Override
                    public Boolean apply( Visit<P> from1, P from2 )
                    {
                        return from1.compareTo( from2 ) >= 0;
                    }
                };
            }
        },

        /* If we are interested in all paths then we must keep around all branches. */
        all
        {
            @Override
            protected <P extends Comparable<P>> Function2<Visit<P>, P, Boolean> interestFunction()
            {
                return new Function2<Visit<P>,P,Boolean>()
                {
                    @Override
                    public Boolean apply( Visit<P> from1, P from2 )
                    {
                        return Boolean.TRUE;
                    }
                };
            }
        };

        protected abstract <P extends Comparable<P>> Function2<Visit<P>,P,Boolean> interestFunction();
    }

    public static PathInterest pathInterest( boolean multiplePaths, boolean stopAfterLowestWeight )
    {
        if ( !multiplePaths )
        {
            return PathInterest.singleLowest;
        }
        return stopAfterLowestWeight ? PathInterest.multipleLowest : PathInterest.all;
    }

    public BestFirstSelectorFactory( PathInterest interest )
    {
        this.interest = interest;
        this.interestPredicate = interest.interestFunction();
    }

    @Override
    public BranchSelector create( TraversalBranch startSource, PathExpander expander )
    {
        return new BestFirstSelector( startSource, getStartData(), expander );
    }

    public BranchSelector create( TraversalBranch startSource, RelationshipExpander expander )
    {
        return new BestFirstSelector( startSource, getStartData(), toPathExpander( expander ) );
    }

    protected abstract P getStartData();

    private static class Visit<P extends Comparable<P>> implements Comparable<P>
    {
        private P cost;
        private boolean visited;

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
        private final PriorityMap<TraversalBranch, Node, P> queue =
                PriorityMap.withNaturalOrder( CONVERTER, false, interest != PathInterest.all );
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
                if ( stay == null || !stay.visited )
                {
                    D cost = calculateValue( next );
                    P newPriority = addPriority( next, currentAggregatedValue, cost );

                    boolean newStay = stay == null;
                    if ( newStay )
                    {
                        stay = new Visit<P>( newPriority );
                        visits.put( endNodeId, stay );
                    }
                    if ( newStay || interestPredicate.apply( stay, newPriority ) )
                    {   // If the priority map already contains a traversal branch with this end node with
                        // a lower cost, then don't bother adding it to the priority map
                        queue.put( next, newPriority );
                        stay.cost = newPriority;
                    }
                }
            }

            // Pop the top from priorityMap
            Entry<TraversalBranch, P> entry = queue.pop();
            if ( entry != null )
            {
                current = entry.getEntity();
                currentAggregatedValue = entry.getPriority();
                visits.get( current.endNode().getId() ).visited = true;
                return current;
            }
            return null;
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

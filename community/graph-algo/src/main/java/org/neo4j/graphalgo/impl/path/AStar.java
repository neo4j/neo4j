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

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.EstimateEvaluator;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphalgo.impl.util.PriorityMap;
import org.neo4j.graphalgo.impl.util.PriorityMap.Entry;
import org.neo4j.graphalgo.impl.util.WeightedPathImpl;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.TraversalMetadata;
import org.neo4j.helpers.collection.PrefetchingIterator;

import static org.neo4j.helpers.collection.Iterables.option;
import static org.neo4j.kernel.StandardExpander.toPathExpander;

public class AStar implements PathFinder<WeightedPath>
{
    private final PathExpander<?> expander;
    private final CostEvaluator<Double> lengthEvaluator;
    private final EstimateEvaluator<Double> estimateEvaluator;
    private Metadata lastMetadata;

    public AStar( RelationshipExpander expander,
            CostEvaluator<Double> lengthEvaluator, EstimateEvaluator<Double> estimateEvaluator )
    {
        this( toPathExpander( expander ), lengthEvaluator, estimateEvaluator );
    }

    public AStar( PathExpander<?> expander,
            CostEvaluator<Double> lengthEvaluator, EstimateEvaluator<Double> estimateEvaluator )
    {
        this.expander = expander;
        this.lengthEvaluator = lengthEvaluator;
        this.estimateEvaluator = estimateEvaluator;
    }

    @Override
    public WeightedPath findSinglePath( Node start, Node end )
    {
        lastMetadata = new Metadata();
        AStarIterator iterator = new AStarIterator( start, end );
        while ( iterator.hasNext() )
        {
            Node node = iterator.next();
            GraphDatabaseService graphDb = node.getGraphDatabase();
            if ( node.equals( end ) )
            {
                // Hit, return path
                double weight = iterator.visitData.get( node.getId() ).wayLength;
                final Path path;
                if ( start.getId() == end.getId() )
                {
                    // Nothing to iterate over
                    path = PathImpl.singular( start );
                }
                else
                {
                    LinkedList<Relationship> rels = new LinkedList<Relationship>();
                    Relationship rel = graphDb.getRelationshipById(
                            iterator.visitData.get( node.getId() ).cameFromRelationship );
                    while ( rel != null )
                    {
                        rels.addFirst( rel );
                        node = rel.getOtherNode( node );
                        long nextRelId = iterator.visitData.get( node.getId() ).cameFromRelationship;
                        rel = nextRelId == -1 ? null : graphDb.getRelationshipById( nextRelId );
                    }
                    path = toPath( start, rels );
                }
                lastMetadata.paths++;
                return new WeightedPathImpl( weight, path );
            }
        }
        return null;
    }

    @Override
    public Iterable<WeightedPath> findAllPaths( final Node node, final Node end )
    {
        return option( findSinglePath( node, end ) );
    }

    @Override
    public TraversalMetadata metadata()
    {
        return lastMetadata;
    }

    private Path toPath( Node start, LinkedList<Relationship> rels )
    {
        PathImpl.Builder builder = new PathImpl.Builder( start );
        for ( Relationship rel : rels )
        {
            builder = builder.push( rel );
        }
        return builder.build();
    }

    private static class Visit
    {
        private double wayLength; // accumulated cost to get here (g)
        private double estimate; // heuristic estimate of cost to reach end (h)
        private long cameFromRelationship;
        private boolean visited;
        private boolean next;

        Visit( long cameFromRelationship, double wayLength, double estimate )
        {
            update( cameFromRelationship, wayLength, estimate );
        }

        void update( long cameFromRelationship, double wayLength, double estimate )
        {
            this.cameFromRelationship = cameFromRelationship;
            this.wayLength = wayLength;
            this.estimate = estimate;
        }

        double getFscore()
        {
            return wayLength + estimate;
        }
    }

    private class AStarIterator extends PrefetchingIterator<Node> implements Path
    {
        private final Node start;
        private final Node end;
        private Node lastNode;
        private final PriorityMap<Node, Node, Double> nextPrioritizedNodes =
                PriorityMap.<Node, Double>withSelfKeyNaturalOrder();
        private final Map<Long, Visit> visitData = new HashMap<Long, Visit>();

        AStarIterator( Node start, Node end )
        {
            this.start = start;
            this.end = end;

            Visit visit = new Visit( -1, 0, estimateEvaluator.getCost( start, end ) );
            addNext( start, visit.getFscore(), visit );
            this.visitData.put( start.getId(), visit );
        }

        private void addNext( Node node, double fscore, Visit visit )
        {
            nextPrioritizedNodes.put( node, fscore );
            visit.next = true;
        }

        private Node popLowestScoreNode()
        {
            Entry<Node, Double> top = nextPrioritizedNodes.pop();
            if ( top == null )
            {
                return null;
            }

            Node node = top.getEntity();
            Visit visit = visitData.get( node.getId() );
            visit.visited = true;
            visit.next = false;
            return node;
        }

        @Override
        protected Node fetchNextOrNull()
        {
            if ( lastNode != null )
            {
                expand();
            }

            return (lastNode = popLowestScoreNode());
        }

        @SuppressWarnings( "unchecked" )
        private void expand()
        {
            Iterable<Relationship> expand = expander.expand( this, BranchState.NO_STATE );
            for ( Relationship rel : expand )
            {
                lastMetadata.rels++;
                Node node = rel.getOtherNode( lastNode );
                Visit visit = visitData.get( node.getId() );
                if ( visit != null && visit.visited )
                {
                    continue;
                }

                Visit lastVisit = visitData.get( lastNode.getId() );
                double tentativeGScore = lastVisit.wayLength +
                        lengthEvaluator.getCost( rel, Direction.OUTGOING );
                double estimate = estimateEvaluator.getCost( node, end );

                if ( visit == null || !visit.next || tentativeGScore < visit.wayLength )
                {
                    if ( visit == null )
                    {
                        visit = new Visit( rel.getId(), tentativeGScore, estimate );
                        visitData.put( node.getId(), visit );
                    }
                    else
                    {
                        visit.update( rel.getId(), tentativeGScore, estimate );
                    }
                    addNext( node, estimate + tentativeGScore, visit );
                }
            }
        }

        @Override
        public Node startNode()
        {
            return start;
        }

        @Override
        public Node endNode()
        {
            return lastNode;
        }

        @Override
        public Relationship lastRelationship()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterable<Relationship> relationships()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterable<Relationship> reverseRelationships()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterable<Node> nodes()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterable<Node> reverseNodes()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int length()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterator<PropertyContainer> iterator()
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class Metadata implements TraversalMetadata
    {
        private int rels;
        private int paths;

        @Override
        public int getNumberOfPathsReturned()
        {
            return paths;
        }

        @Override
        public int getNumberOfRelationshipsTraversed()
        {
            return rels;
        }
    }
}

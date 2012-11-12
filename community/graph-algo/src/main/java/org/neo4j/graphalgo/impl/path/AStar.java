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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.EstimateEvaluator;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphalgo.impl.util.PathImpl;
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
    
    public WeightedPath findSinglePath( Node start, Node end )
    {
        lastMetadata = new Metadata();
        Doer doer = new Doer( start, end );
        while ( doer.hasNext() )
        {
            Node node = doer.next();
            GraphDatabaseService graphDb = node.getGraphDatabase();
            if ( node.equals( end ) )
            {
                // Hit, return path
                double weight = doer.score.get( node.getId() ).wayLength;
                LinkedList<Relationship> rels = new LinkedList<Relationship>();
                Relationship rel = graphDb.getRelationshipById( doer.cameFrom.get( node.getId() ) );
                while ( rel != null )
                {
                    rels.addFirst( rel );
                    node = rel.getOtherNode( node );
                    Long nextRelId = doer.cameFrom.get( node.getId() );
                    rel = nextRelId == null ? null : graphDb.getRelationshipById( nextRelId );
                }
                Path path = toPath( start, rels );
                lastMetadata.paths++;
                return new WeightedPathImpl( weight, path );
            }
        }
        return null;
    }
    
    public Iterable<WeightedPath> findAllPaths( Node node, Node end )
    {
        WeightedPath path = findSinglePath( node, end );
        return path != null ? Arrays.asList( path ) : Collections.<WeightedPath>emptyList();
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
    
    private static class Data
    {
        private double wayLength; // acumulated cost to get here (g)
        private double estimate; // heuristic estimate of cost to reach end (h)
        
        double getFscore()
        {
            return wayLength + estimate;
        }
    }

    private class Doer extends PrefetchingIterator<Node> implements Path
    {
        private final Node end;
        private Node lastNode;
        private boolean expand;
        private final Set<Long> visitedNodes = new HashSet<Long>();
        private final Set<Node> nextNodesSet = new HashSet<Node>();
        private final TreeMap<Double, Collection<Node>> nextNodes =
                new TreeMap<Double, Collection<Node>>();
        private final Map<Long, Long> cameFrom = new HashMap<Long, Long>();
        private final Map<Long, Data> score = new HashMap<Long, Data>();
        private final Node start;
        
        Doer( Node start, Node end )
        {
            this.start = start;
            this.end = end;
            
            Data data = new Data();
            data.wayLength = 0;
            data.estimate = estimateEvaluator.getCost( start, end );
            addNext( start, data.getFscore() );
            this.score.put( start.getId(), data );
        }
        
        private void addNext( Node node, double fscore )
        {
            Collection<Node> nodes = this.nextNodes.get( fscore );
            if ( nodes == null )
            {
                nodes = new HashSet<Node>();
                this.nextNodes.put( fscore, nodes );
            }
            nodes.add( node );
            this.nextNodesSet.add( node );
        }

        private Node popLowestScoreNode()
        {
            Iterator<Map.Entry<Double, Collection<Node>>> itr =
                    this.nextNodes.entrySet().iterator();
            if ( !itr.hasNext() )
            {
                return null;
            }
            
            Map.Entry<Double, Collection<Node>> entry = itr.next();
            Node node = entry.getValue().isEmpty() ? null : entry.getValue().iterator().next();
            if ( node == null )
            {
                return null;
            }
            
            if ( node != null )
            {
                entry.getValue().remove( node );
                this.nextNodesSet.remove( node );
                if ( entry.getValue().isEmpty() )
                {
                    this.nextNodes.remove( entry.getKey() );
                }
                this.visitedNodes.add( node.getId() );
            }
            return node;
        }

        @Override
        protected Node fetchNextOrNull()
        {
            // FIXME
            if ( !this.expand )
            {
                this.expand = true;
            }
            else
            {
                expand();
            }
            
            Node node = popLowestScoreNode();
            this.lastNode = node;
            return node;
        }

        @SuppressWarnings( "unchecked" )
        private void expand()
        {
            for ( Relationship rel : expander.expand( this, BranchState.NO_STATE ) )
            {
                lastMetadata.rels++;
                Node node = rel.getOtherNode( this.lastNode );
                if ( this.visitedNodes.contains( node.getId() ) )
                {
                    continue;
                }
                
                Data lastNodeData = this.score.get( this.lastNode.getId() );
                double tentativeGScore = lastNodeData.wayLength +
                        lengthEvaluator.getCost( rel, Direction.OUTGOING );
                boolean isBetter = false;
                double estimate = estimateEvaluator.getCost( node, this.end );
                if ( !this.nextNodesSet.contains( node ) )
                {
                    addNext( node, estimate + tentativeGScore );
                    isBetter = true;
                }
                else if ( tentativeGScore < this.score.get( node.getId() ).wayLength )
                {
                    isBetter = true;
                }
                
                if ( isBetter )
                {
                    this.cameFrom.put( node.getId(), rel.getId() );
                    Data data = new Data();
                    data.wayLength = tentativeGScore;
                    data.estimate = estimate;
                    this.score.put( node.getId(), data );
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

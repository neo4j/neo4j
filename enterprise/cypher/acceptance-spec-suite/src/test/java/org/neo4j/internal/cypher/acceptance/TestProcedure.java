/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.internal.cypher.acceptance;

import java.time.Duration;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static org.neo4j.graphdb.traversal.Evaluation.EXCLUDE_AND_CONTINUE;
import static org.neo4j.graphdb.traversal.Evaluation.EXCLUDE_AND_PRUNE;
import static org.neo4j.graphdb.traversal.Evaluation.INCLUDE_AND_CONTINUE;
import static org.neo4j.procedure.Mode.WRITE;

public class TestProcedure
{
    @Context
    public GraphDatabaseService db;

    @Procedure( "org.neo4j.time" )
    @Description( "org.neo4j.time" )
    public void time( @Name( value = "time" ) LocalTime statementTime )
    {
        LocalTime realTime = LocalTime.now();
        Duration duration = Duration.between( statementTime, realTime );
    }

    @Procedure( "org.neo4j.aNodeWithLabel" )
    @Description( "org.neo4j.aNodeWithLabel" )
    public Stream<NodeResult> aNodeWithLabel( @Name( value = "label", defaultValue = "Dog" ) String label )
    {
        Result result = db.execute( "MATCH (n:" + label + ") RETURN n LIMIT 1" );
        return result.stream().map( row -> new NodeResult( (Node)row.get( "n" ) ) );
    }

    @Procedure( "org.neo4j.stream123" )
    @Description( "org.neo4j.stream123" )
    public Stream<CountResult> stream123()
    {
        return IntStream.of( 1, 2, 3 ).mapToObj( i -> new CountResult( i, "count" + i ) );
    }

    @Procedure( "org.neo4j.recurseN" )
    @Description( "org.neo4j.recurseN" )
    public Stream<NodeResult> recurseN( @Name( "n" ) Long n )
    {
        Result result;
        if ( n == 0 )
        {
            result = db.execute( "MATCH (n) RETURN n LIMIT 1" );
        }
        else
        {
            result = db.execute( "UNWIND [1] AS i CALL org.neo4j.recurseN(" + ( n - 1 ) + ") YIELD node RETURN node" );
        }
        return result.stream().map( row -> new NodeResult( (Node)row.get( "node" ) ) );
    }

    @Procedure( "org.neo4j.findNodesWithLabel" )
    @Description( "org.neo4j.findNodesWithLabel" )
    public Stream<NodeResult> findNodesWithLabel( @Name( "label" ) String label )
    {
        ResourceIterator<Node> nodes = db.findNodes( Label.label( label ) );
        return nodes.stream().map( NodeResult::new );
    }

    @Procedure( "org.neo4j.expandNode" )
    @Description( "org.neo4j.expandNode" )
    public Stream<NodeResult> expandNode( @Name( "nodeId" ) Long nodeId )
    {
        Node node = db.getNodeById( nodeId );
        List<Node> result = new ArrayList<>();
        for ( Relationship r : node.getRelationships() )
        {
            result.add( r.getOtherNode( node ) );
        }

        return result.stream().map( NodeResult::new );
    }

    @Procedure( name = "org.neo4j.createNodeWithLoop", mode = WRITE )
    @Description( "org.neo4j.createNodeWithLoop" )
    public Stream<NodeResult> createNodeWithLoop(
            @Name( "nodeLabel" ) String label, @Name( "relType" ) String relType )
    {
        Node node = db.createNode( Label.label( label ) );
        node.createRelationshipTo( node, RelationshipType.withName( relType ) );
        return Stream.of( new NodeResult( node ) );
    }

    @Procedure( name = "org.neo4j.graphAlgosDjikstra" )
    @Description( "org.neo4j.graphAlgosDjikstra" )
    public Stream<NodeResult> graphAlgosDjikstra(
            @Name( "startNode" ) Node start,
            @Name( "endNode" ) Node end,
            @Name( "relType" ) String relType,
            @Name( "weightProperty" ) String weightProperty )
    {
        PathFinder<WeightedPath> pathFinder =
                GraphAlgoFactory.dijkstra(
                        PathExpanders.forTypeAndDirection(
                                RelationshipType.withName( relType ), Direction.BOTH ),
                        weightProperty );

        WeightedPath path = pathFinder.findSinglePath( start, end );
        return StreamSupport.stream( path.nodes().spliterator(), false ).map( NodeResult::new  );
    }

    public static class NodeResult
    {
        public Node node;

        NodeResult( Node node )
        {
            this.node = node;
        }
    }

    public static class CountResult
    {
        public long count;
        public String name;

        CountResult( long count, String name )
        {
            this.count = count;
            this.name = name;
        }
    }

    @Procedure( "org.neo4j.movieTraversal" )
    @Description( "org.neo4j.movieTraversal" )
    public Stream<PathResult> movieTraversal( @Name( "start" ) Node start )
    {
        TraversalDescription td =
                db.traversalDescription()
                        .breadthFirst()
                        .relationships( RelationshipType.withName( "ACTED_IN" ), Direction.BOTH )
                        .relationships( RelationshipType.withName( "PRODUCED" ), Direction.BOTH )
                        .relationships( RelationshipType.withName( "DIRECTED" ), Direction.BOTH )
                        .evaluator( Evaluators.fromDepth( 3 ) )
                        .evaluator( new LabelEvaluator( "Western", 1, 3 ) )
                        .uniqueness( Uniqueness.NODE_GLOBAL );

        return td.traverse( start ).stream().map( PathResult::new );
    }

    public static class PathResult
    {
        public Path path;

        PathResult( Path path )
        {
            this.path = path;
        }
    }

    public static class LabelEvaluator implements Evaluator
    {
        private Set<String> endNodeLabels;
        private long limit = -1;
        private long minLevel = -1;
        private long resultCount;

        LabelEvaluator( String endNodeLabel, long limit, int minLevel )
        {
            this.limit = limit;
            this.minLevel = minLevel;

            endNodeLabels = Collections.singleton( endNodeLabel );
        }

        @Override
        public Evaluation evaluate( Path path )
        {
            int depth = path.length();
            Node check = path.endNode();

            if ( depth < minLevel )
            {
                return EXCLUDE_AND_CONTINUE;
            }

            if ( limit != -1 && resultCount >= limit )
            {
                return EXCLUDE_AND_PRUNE;
            }

            return labelExists( check, endNodeLabels ) ? countIncludeAndContinue() : EXCLUDE_AND_CONTINUE;
        }

        private boolean labelExists( Node node, Set<String> labels )
        {
            if ( labels.isEmpty() )
            {
                return false;
            }

            for ( Label lab : node.getLabels() )
            {
                if ( labels.contains( lab.name() ) )
                {
                    return true;
                }
            }
            return false;
        }

        private Evaluation countIncludeAndContinue()
        {
            resultCount++;
            return INCLUDE_AND_CONTINUE;
        }
    }
}

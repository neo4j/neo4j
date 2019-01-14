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
package org.neo4j.internal.kernel.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.RelationshipType.withName;

public class RelationshipTestSupport
{
    private RelationshipTestSupport()
    {
    }

    static void someGraph( GraphDatabaseService graphDb )
    {
        Relationship dead;
        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            Node a = graphDb.createNode(),
                    b = graphDb.createNode(),
                    c = graphDb.createNode(),
                    d = graphDb.createNode();

            a.createRelationshipTo( a, withName( "ALPHA" ) );
            a.createRelationshipTo( b, withName( "BETA" ) );
            a.createRelationshipTo( c, withName( "GAMMA" ) );
            a.createRelationshipTo( d, withName( "DELTA" ) );

            graphDb.createNode().createRelationshipTo( a, withName( "BETA" ) );
            a.createRelationshipTo( graphDb.createNode(), withName( "BETA" ) );
            dead = a.createRelationshipTo( graphDb.createNode(), withName( "BETA" ) );
            a.createRelationshipTo( graphDb.createNode(), withName( "BETA" ) );

            Node clump = graphDb.createNode();
            clump.createRelationshipTo( clump, withName( "REL" ) );
            clump.createRelationshipTo( clump, withName( "REL" ) );
            clump.createRelationshipTo( clump, withName( "REL" ) );
            clump.createRelationshipTo( graphDb.createNode(), withName( "REL" ) );
            clump.createRelationshipTo( graphDb.createNode(), withName( "REL" ) );
            clump.createRelationshipTo( graphDb.createNode(), withName( "REL" ) );
            graphDb.createNode().createRelationshipTo( clump, withName( "REL" ) );
            graphDb.createNode().createRelationshipTo( clump, withName( "REL" ) );
            graphDb.createNode().createRelationshipTo( clump, withName( "REL" ) );

            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            Node node = dead.getEndNode();
            dead.delete();
            node.delete();

            tx.success();
        }
    }

    static StartNode sparse( GraphDatabaseService graphDb )
    {
        Node node;
        Map<String,List<StartRelationship>> relationshipMap;
        try ( Transaction tx = graphDb.beginTx() )
        {
            node = graphDb.createNode();
            relationshipMap = buildSparseDenseRels( node );
            tx.success();
        }
        return new StartNode( node.getId(), relationshipMap );
    }

    static StartNode dense( GraphDatabaseService graphDb )
    {
        Node node;
        Map<String,List<StartRelationship>> relationshipMap;
        try ( Transaction tx = graphDb.beginTx() )
        {
            node = graphDb.createNode();
            relationshipMap = buildSparseDenseRels( node );

            List<StartRelationship> bulk = new ArrayList<>();
            RelationshipType bulkType = withName( "BULK" );

            for ( int i = 0; i < 200; i++ )
            {
                Relationship r = node.createRelationshipTo( graphDb.createNode(), bulkType );
                bulk.add( new StartRelationship( r.getId(), Direction.OUTGOING, bulkType ) );
            }

            String bulkKey = computeKey( "BULK", Direction.OUTGOING );
            relationshipMap.put( bulkKey, bulk );

            tx.success();
        }
        return new StartNode( node.getId(), relationshipMap );
    }

    static Map<String,Integer> count(
            org.neo4j.internal.kernel.api.Transaction transaction,
            RelationshipTraversalCursor relationship ) throws KernelException
    {
        HashMap<String,Integer> counts = new HashMap<>();
        while ( relationship.next() )
        {
            String key = computeKey( transaction, relationship );
            counts.compute( key, ( k, value ) -> value == null ? 1 : value + 1 );
        }
        return counts;
    }

    static void assertCount(
            org.neo4j.internal.kernel.api.Transaction transaction,
            RelationshipTraversalCursor relationship,
            Map<String,Integer> expectedCounts,
            int expectedType,
            Direction direction ) throws KernelException
    {
        String key = computeKey( transaction.token().relationshipTypeName( expectedType ), direction );
        int expectedCount = expectedCounts.getOrDefault( key, 0 );
        int count = 0;

        while ( relationship.next() )
        {
            assertEquals( "same type", expectedType, relationship.type() );
            count++;
        }

        assertEquals( format( "expected number of relationships for key '%s'", key ), expectedCount, count );
    }

    static class StartRelationship
    {
        public final long id;
        public final Direction direction;
        public final RelationshipType type;

        StartRelationship( long id, Direction direction, RelationshipType type )
        {
            this.id = id;
            this.type = type;
            this.direction = direction;
        }
    }

    static class StartNode
    {
        public final long id;
        public final Map<String,List<StartRelationship>> relationships;

        StartNode( long id, Map<String,List<StartRelationship>> relationships )
        {
            this.id = id;
            this.relationships = relationships;
        }

        Map<String,Integer> expectedCounts()
        {
            Map<String,Integer> expectedCounts = new HashMap<>();
            for ( Map.Entry<String,List<StartRelationship>> kv : relationships.entrySet() )
            {
                expectedCounts.put( kv.getKey(), relationships.get( kv.getKey() ).size() );
            }
            return expectedCounts;
        }
    }

    static void assertCounts( Map<String,Integer> expectedCounts, Map<String,Integer> counts )
    {
        for ( Map.Entry<String, Integer> expected : expectedCounts.entrySet() )
        {
            assertEquals(
                    format( "counts for relationship key '%s' are equal", expected.getKey() ),
                    expected.getValue(), counts.get( expected.getKey()) );
        }
    }

    private static Map<String,List<StartRelationship>> buildSparseDenseRels( Node node )
    {
        Map<String,List<StartRelationship>> relationshipMap = new HashMap<>();
        for ( Function<Node,StartRelationship> rel : sparseDenseRels )
        {
            StartRelationship r = rel.apply( node );
            List<StartRelationship> relsOfType = relationshipMap.computeIfAbsent( computeKey( r ), key -> new ArrayList<>() );
            relsOfType.add( r );
        }
        return relationshipMap;
    }

    private static String computeKey( StartRelationship r )
    {
        return computeKey( r.type.name(), r.direction );
    }

    private static String computeKey( org.neo4j.internal.kernel.api.Transaction transaction, RelationshipTraversalCursor r ) throws KernelException
    {
        Direction d;
        if ( r.sourceNodeReference() == r.targetNodeReference() )
        {
            d = Direction.BOTH;
        }
        else if ( r.sourceNodeReference() == r.originNodeReference() )
        {
            d = Direction.OUTGOING;
        }
        else
        {
            d = Direction.INCOMING;
        }

        return computeKey( transaction.token().relationshipTypeName( r.type() ), d );
    }

    static String computeKey( String type, Direction direction )
    {
        return type + "-" + direction;
    }

    private static Function<Node,StartRelationship>[] sparseDenseRels = Iterators.array(
            loop( "FOO" ), // loops are the hardest, let's have two to try to interfere with outgoing/incoming code
            outgoing( "FOO" ),
            outgoing( "BAR" ),
            outgoing( "BAR" ),
            incoming( "FOO" ),
            outgoing( "FOO" ),
            incoming( "BAZ" ),
            incoming( "BAR" ),
            outgoing( "BAZ" ),
            loop( "FOO" )
    );

    private static Function<Node,StartRelationship> outgoing( String type )
    {
        return node ->
        {
            GraphDatabaseService db = node.getGraphDatabase();
            RelationshipType relType = withName( type );
            return new StartRelationship(
                    node.createRelationshipTo( db.createNode(), relType ).getId(),
                    Direction.OUTGOING,
                    relType );
        };
    }

    private static Function<Node,StartRelationship> incoming( String type )
    {
        return node ->
        {
            GraphDatabaseService db = node.getGraphDatabase();
            RelationshipType relType = withName( type );
            return new StartRelationship(
                    db.createNode().createRelationshipTo( node, relType ).getId(),
                    Direction.INCOMING,
                    relType );
        };
    }

    private static Function<Node,StartRelationship> loop( String type )
    {
        return node ->
        {
            RelationshipType relType = withName( type );
            return new StartRelationship(
                    node.createRelationshipTo( node, relType ).getId(),
                    Direction.BOTH,
                    relType );
        };
    }
}

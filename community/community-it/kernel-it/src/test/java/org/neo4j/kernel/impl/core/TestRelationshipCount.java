/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.core;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collection;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.IterableWrapper;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class TestRelationshipCount
{
    private static DatabaseManagementService managementService;

    private static Stream<Arguments> argumentsProvider()
    {
        int max = GraphDatabaseSettings.dense_node_threshold.defaultValue();
        return IntStream.range( 1, max ).mapToObj( i -> Arguments.of( i ) );
    }

    private static GraphDatabaseAPI db;
    private Transaction tx;

    @AfterAll
    public static void shutdownDb()
    {
        managementService.shutdown();
    }

    public void init( final int denseNodeThreshold )
    {
        // This code below basically turns "db" into a ClassRule, but per dense node threshold
        if ( db == null || db.getDependencyResolver().resolveDependency( Config.class )
                .get( GraphDatabaseSettings.dense_node_threshold ) != denseNodeThreshold )
        {
            if ( db != null )
            {
                managementService.shutdown();
            }
            managementService = new TestDatabaseManagementServiceBuilder().impermanent()
                        .setConfig( GraphDatabaseSettings.dense_node_threshold, denseNodeThreshold ).build();
            db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        }

        newTransaction();
    }

    public void newTransaction()
    {
        if ( tx != null )
        {
            closeTransaction();
        }
        tx = getGraphDb().beginTx();
    }

    @AfterEach
    public void closeTransaction()
    {
        assert tx != null;
        tx.commit();
    }

    private GraphDatabaseService getGraphDb()
    {
        return db;
    }

    @ParameterizedTest( name = "denseNodeThreshold={0}" )
    @MethodSource( "argumentsProvider" )
    public void convertNodeToDense( int denseNodeThreshold )
    {
        init( denseNodeThreshold );

        Node node = tx.createNode();
        EnumMap<MyRelTypes,Set<Relationship>> rels = new EnumMap<>( MyRelTypes.class );
        for ( MyRelTypes type : MyRelTypes.values() )
        {
            rels.put( type, new HashSet<>() );
        }
        int expectedRelCount = 0;
        for ( int i = 0; i < 6; i++, expectedRelCount++ )
        {
            MyRelTypes type = MyRelTypes.values()[i % MyRelTypes.values().length];
            Relationship rel = node.createRelationshipTo( tx.createNode(), type );
            rels.get( type ).add( rel );
        }
        newTransaction();
        node = tx.getNodeById( node.getId() );
        for ( int i = 0; i < 1000; i++, expectedRelCount++ )
        {
            node.createRelationshipTo( tx.createNode(), MyRelTypes.TEST );
        }

        assertEquals( expectedRelCount, node.getDegree() );
        assertEquals( expectedRelCount, node.getDegree( Direction.BOTH ) );
        assertEquals( expectedRelCount, node.getDegree( Direction.OUTGOING ) );
        assertEquals( 0, node.getDegree( Direction.INCOMING ) );
        assertEquals( rels.get( MyRelTypes.TEST2 ), Iterables.asSet( node.getRelationships( MyRelTypes.TEST2 ) ) );
        assertEquals( join( rels.get( MyRelTypes.TEST_TRAVERSAL ), rels.get( MyRelTypes.TEST2 ) ),
                Iterables.asSet( node.getRelationships( MyRelTypes.TEST_TRAVERSAL, MyRelTypes.TEST2 ) ) );
    }

    private <T> Set<T> join( Set<T> set, Set<T> set2 )
    {
        Set<T> result = new HashSet<>( set );
        result.addAll( set2 );
        return result;
    }

    @ParameterizedTest( name = "denseNodeThreshold={0}" )
    @MethodSource( "argumentsProvider" )
    public void testGetRelationshipTypesOnDiscreteNode( int denseNodeThreshold )
    {
        init( denseNodeThreshold );

        testGetRelationshipTypes( tx.createNode(), new HashSet<>() );
    }

    @ParameterizedTest( name = "denseNodeThreshold={0}" )
    @MethodSource( "argumentsProvider" )
    public void testGetRelationshipTypesOnDenseNode( int denseNodeThreshold )
    {
        init( denseNodeThreshold );

        Node node = tx.createNode();
        Node otherNode = tx.createNode();
        for ( int i = 0; i < 300; i++ )
        {
            node.createRelationshipTo( otherNode, RelType.INITIAL );
        }
        testGetRelationshipTypes( node, new HashSet<>( List.of( RelType.INITIAL.name() ) ) );
    }

    private void testGetRelationshipTypes( Node node, Set<String> expectedTypes )
    {
        assertExpectedRelationshipTypes( expectedTypes, node, false );
        node.createRelationshipTo( tx.createNode(), RelType.TYPE1 );
        expectedTypes.add( RelType.TYPE1.name() );
        assertExpectedRelationshipTypes( expectedTypes, node, false );
        node.createRelationshipTo( tx.createNode(), RelType.TYPE1 );
        assertExpectedRelationshipTypes( expectedTypes, node, true );

        node = tx.getNodeById( node.getId() );
        Relationship rel = node.createRelationshipTo( tx.createNode(), RelType.TYPE2 );
        expectedTypes.add( RelType.TYPE2.name() );
        assertExpectedRelationshipTypes( expectedTypes, node, false );
        rel.delete();
        expectedTypes.remove( RelType.TYPE2.name() );
        assertExpectedRelationshipTypes( expectedTypes, node, true );

        node = tx.getNodeById( node.getId() );
        node.createRelationshipTo( tx.createNode(), RelType.TYPE2 );
        node.createRelationshipTo( tx.createNode(), RelType.TYPE2 );
        expectedTypes.add( RelType.TYPE2.name() );
        node.createRelationshipTo( tx.createNode(), MyRelTypes.TEST );
        expectedTypes.add( MyRelTypes.TEST.name() );
        assertExpectedRelationshipTypes( expectedTypes, node, true );

        node = tx.getNodeById( node.getId() );
        for ( Relationship r : node.getRelationships( RelType.TYPE1 ) )
        {
            assertExpectedRelationshipTypes( expectedTypes, node, false );
            r.delete();
        }
        expectedTypes.remove( RelType.TYPE1.name() );
        assertExpectedRelationshipTypes( expectedTypes, node, true );
    }

    private void assertExpectedRelationshipTypes( Set<String> expectedTypes, Node node, boolean commit )
    {
        Set<String> actual = Iterables.asSet( asStrings( node.getRelationshipTypes() ) );
        assertEquals( expectedTypes, actual );
        if ( commit )
        {
            newTransaction();
        }
        assertEquals( expectedTypes, Iterables.asSet( asStrings( tx.getNodeById( node.getId() ).getRelationshipTypes() ) ) );
    }

    private Iterable<String> asStrings( Iterable<RelationshipType> relationshipTypes )
    {
        return new IterableWrapper<>( relationshipTypes )
        {
            @Override
            protected String underlyingObjectToObject( RelationshipType object )
            {
                return object.name();
            }
        };
    }

    @ParameterizedTest( name = "denseNodeThreshold={0}" )
    @MethodSource( "argumentsProvider" )
    public void withoutLoops( int denseNodeThreshold )
    {
        init( denseNodeThreshold );

        Node node1 = tx.createNode();
        Node node2 = tx.createNode();
        assertEquals( 0, node1.getDegree() );
        assertEquals( 0, node2.getDegree() );
        node1.createRelationshipTo( node2, MyRelTypes.TEST );
        assertEquals( 1, node1.getDegree() );
        assertEquals( 1, node2.getDegree() );
        node1.createRelationshipTo( tx.createNode(), MyRelTypes.TEST2 );
        assertEquals( 2, node1.getDegree() );
        assertEquals( 1, node2.getDegree() );
        newTransaction();

        node1 = tx.getNodeById( node1.getId() );
        node2 = tx.getNodeById( node2.getId() );
        assertEquals( 2, node1.getDegree() );
        assertEquals( 1, node2.getDegree() );

        for ( int i = 0; i < 1000; i++ )
        {
            if ( i % 2 == 0 )
            {
                node1.createRelationshipTo( node2, MyRelTypes.TEST );
            }
            else
            {
                node2.createRelationshipTo( node1, MyRelTypes.TEST );
            }
            assertEquals( i + 2 + 1, node1.getDegree() );
            assertEquals( i + 1 + 1, node2.getDegree() );
            if ( i % 10 == 0 )
            {
                newTransaction();
                node1 = tx.getNodeById( node1.getId() );
                node2 = tx.getNodeById( node2.getId() );
            }
        }

        node1 = tx.getNodeById( node1.getId() );
        node2 = tx.getNodeById( node2.getId() );
        for ( int i = 0; i < 2; i++ )
        {
            assertEquals( 1002, node1.getDegree() );
            assertEquals( 1002, node1.getDegree( Direction.BOTH ) );
            assertEquals( 502, node1.getDegree( Direction.OUTGOING ) );
            assertEquals( 500, node1.getDegree( Direction.INCOMING ) );
            assertEquals( 1, node1.getDegree( MyRelTypes.TEST2 ) );
            assertEquals( 1001, node1.getDegree( MyRelTypes.TEST ) );

            assertEquals( 1001, node1.getDegree( MyRelTypes.TEST, Direction.BOTH ) );
            assertEquals( 501, node1.getDegree( MyRelTypes.TEST, Direction.OUTGOING ) );
            assertEquals( 500, node1.getDegree( MyRelTypes.TEST, Direction.INCOMING ) );
            assertEquals( 1, node1.getDegree( MyRelTypes.TEST2, Direction.OUTGOING ) );
            assertEquals( 0, node1.getDegree( MyRelTypes.TEST2, Direction.INCOMING ) );
            newTransaction();
            node1 = tx.getNodeById( node1.getId() );
            node2 = tx.getNodeById( node2.getId() );
        }

        for ( Relationship rel : node1.getRelationships() )
        {
            rel.delete();
        }
        node1.delete();
        for ( Relationship rel : node2.getRelationships() )
        {
            rel.delete();
        }
        node2.delete();
        newTransaction();
    }

    @ParameterizedTest( name = "denseNodeThreshold={0}" )
    @MethodSource( "argumentsProvider" )
    public void withLoops( int denseNodeThreshold )
    {
        init( denseNodeThreshold );

        // Just to make sure it doesn't work by accident what with ids aligning with count
        for ( int i = 0; i < 10; i++ )
        {
            tx.createNode().createRelationshipTo( tx.createNode(), MyRelTypes.TEST );
        }

        Node node = tx.createNode();
        assertEquals( 0, node.getDegree() );
        node.createRelationshipTo( node, MyRelTypes.TEST );
        assertEquals( 1, node.getDegree() );
        Node otherNode = tx.createNode();
        Relationship rel2 = node.createRelationshipTo( otherNode, MyRelTypes.TEST2 );
        assertEquals( 2, node.getDegree() );
        assertEquals( 1, otherNode.getDegree() );
        newTransaction();
        node = tx.getNodeById( node.getId() );
        otherNode = tx.getNodeById( otherNode.getId() );
        rel2 = tx.getRelationshipById( rel2.getId() );
        assertEquals( 2, node.getDegree() );
        Relationship rel3 = node.createRelationshipTo( node, MyRelTypes.TEST_TRAVERSAL );
        assertEquals( 3, node.getDegree() );
        assertEquals( 1, otherNode.getDegree() );
        rel2.delete();
        assertEquals( 2, node.getDegree() );
        assertEquals( 0, otherNode.getDegree() );
        rel3.delete();
        assertEquals( 1, node.getDegree() );
    }

    @ParameterizedTest( name = "denseNodeThreshold={0}" )
    @MethodSource( "argumentsProvider" )
    public void ensureRightDegree( int denseNodeThreshold )
    {
        init( denseNodeThreshold );

        for ( int initialSize : new int[] { 0, 95, 200 } )
        {
            ensureRightDegree( initialSize,
                    asList(
                    create( RelType.TYPE1, Direction.OUTGOING, 5 ),
                    create( RelType.TYPE1, Direction.INCOMING, 2 ),
                    create( RelType.TYPE2, Direction.OUTGOING, 6 ),
                    create( RelType.TYPE2, Direction.INCOMING, 7 ),
                    create( RelType.TYPE2, Direction.BOTH, 3 ) ),

                    asList(
                    delete( RelType.TYPE1, Direction.OUTGOING, 0 ),
                    delete( RelType.TYPE1, Direction.INCOMING, 1 ),
                    delete( RelType.TYPE2, Direction.OUTGOING, Integer.MAX_VALUE ),
                    delete( RelType.TYPE2, Direction.INCOMING, 1 ),
                    delete( RelType.TYPE2, Direction.BOTH, Integer.MAX_VALUE ) )/*null*/ );

            ensureRightDegree( initialSize,
                    asList(
                    create( RelType.TYPE1, Direction.BOTH, 1 ),
                    create( RelType.TYPE1, Direction.OUTGOING, 5 ),
                    create( RelType.TYPE2, Direction.OUTGOING, 6 ),
                    create( RelType.TYPE1, Direction.INCOMING, 2 ),
                    create( RelType.TYPE2, Direction.BOTH, 3 ),
                    create( RelType.TYPE2, Direction.INCOMING, 7 ),
                    create( RelType.TYPE2, Direction.BOTH, 3 ) ), null );

            ensureRightDegree( initialSize,
                    asList(
                    create( RelType.TYPE1, Direction.BOTH, 2 ),
                    create( RelType.TYPE2, Direction.BOTH, 1 ),
                    create( RelType.TYPE1, Direction.OUTGOING, 1 ),
                    create( RelType.TYPE2, Direction.OUTGOING, 10 ),
                    create( RelType.TYPE1, Direction.INCOMING, 2 ),
                    create( RelType.TYPE2, Direction.BOTH, 2 ),
                    create( RelType.TYPE2, Direction.INCOMING, 7 ) ), null );
        }
    }

    private void ensureRightDegree( int initialSize, Collection<RelationshipCreationSpec> cspecs,
            Collection<RelationshipDeletionSpec> dspecs )
    {
        Map<RelType, int[]> expectedCounts = new EnumMap<>( RelType.class );
        for ( RelType type : RelType.values() )
        {
            expectedCounts.put( type, new int[3] );
        }
        Node me = tx.createNode();
        for ( int i = 0; i < initialSize; i++ )
        {
            me.createRelationshipTo( tx.createNode(), RelType.INITIAL );
        }
        newTransaction();
        me = tx.getNodeById( me.getId() );
        expectedCounts.get( RelType.INITIAL )[0] = initialSize;

        assertCounts( me, expectedCounts );
        int counter = 0;
        for ( RelationshipCreationSpec spec : cspecs )
        {
            for ( int i = 0; i < spec.count; i++ )
            {
                Node otherNode = null;
                if ( spec.dir == Direction.OUTGOING )
                {
                    otherNode = tx.createNode();
                    me.createRelationshipTo( otherNode, spec.type );
                }
                else if ( spec.dir == Direction.INCOMING )
                {
                    otherNode = tx.createNode();
                    otherNode.createRelationshipTo( me, spec.type );
                }
                else
                {
                    me.createRelationshipTo( me, spec.type );
                }
                expectedCounts.get( spec.type )[spec.dir.ordinal()]++;

                if ( otherNode != null )
                {
                    assertEquals( 1, otherNode.getDegree() );
                }
                assertCounts( me, expectedCounts );
                if ( counter % 3 == 0 && counter > 0 )
                {
                    newTransaction();
                    assertCounts( me, expectedCounts );
                }
            }
        }

        assertCounts( me, expectedCounts );
        newTransaction();
        me = tx.getNodeById( me.getId() );
        assertCounts( me, expectedCounts );

        // Delete one of each type/direction combination
        counter = 0;
        if ( dspecs == null )
        {
            for ( RelType type : RelType.values() )
            {
                if ( !type.measure )
                {
                    continue;
                }
                for ( Direction direction : Direction.values() )
                {
                    int[] counts = expectedCounts.get( type );
                    if ( counts[direction.ordinal()] > 0 )
                    {
                        deleteOneRelationship( me, type, direction, 0 );
                        counts[direction.ordinal()]--;
                        assertCounts( me, expectedCounts );
                        if ( counter % 3 == 0 && counter > 0 )
                        {
                            newTransaction();
                            assertCounts( me, expectedCounts );
                        }
                    }
                }
            }
        }
        else
        {
            for ( RelationshipDeletionSpec spec : dspecs )
            {
                deleteOneRelationship( me, spec.type, spec.dir, spec.which );
                expectedCounts.get( spec.type )[spec.dir.ordinal()]--;
                assertCounts( me, expectedCounts );
                if ( counter % 3 == 0 && counter > 0 )
                {
                    newTransaction();
                    assertCounts( me, expectedCounts );
                }
            }
        }

        assertCounts( me, expectedCounts );
        newTransaction();
        me = tx.getNodeById( me.getId() );
        assertCounts( me, expectedCounts );

        // Clean up
        for ( Relationship rel : me.getRelationships() )
        {
            Node otherNode = rel.getOtherNode( me );
            if ( !otherNode.equals( me ) )
            {
                otherNode.delete();
            }
            rel.delete();
        }
        me.delete();
    }

    private void assertCounts( Node me, Map<RelType, int[]> expectedCounts )
    {
        assertEquals( totalCount( expectedCounts, Direction.BOTH ), me.getDegree() );
        assertEquals( totalCount( expectedCounts, Direction.BOTH ), me.getDegree( Direction.BOTH ) );
        assertEquals( totalCount( expectedCounts, Direction.OUTGOING ), me.getDegree( Direction.OUTGOING ) );
        assertEquals( totalCount( expectedCounts, Direction.INCOMING ), me.getDegree( Direction.INCOMING ) );
        for ( Map.Entry<RelType, int[]> entry : expectedCounts.entrySet() )
        {
            RelType type = entry.getKey();
            assertEquals( totalCount( entry.getValue(), Direction.BOTH ), me.getDegree( type ) );
            assertEquals( totalCount( entry.getValue(), Direction.OUTGOING ), me.getDegree( type, Direction.OUTGOING ) );
            assertEquals( totalCount( entry.getValue(), Direction.INCOMING ), me.getDegree( type, Direction.INCOMING ) );
            assertEquals( totalCount( entry.getValue(), Direction.BOTH ), me.getDegree( type, Direction.BOTH ) );
        }
    }

    private int totalCount( Map<RelType, int[]> expectedCounts, Direction direction )
    {
        int result = 0;
        for ( Map.Entry<RelType, int[]> entry : expectedCounts.entrySet() )
        {
            result += totalCount( entry.getValue(), direction );
        }
        return result;
    }

    private int totalCount( int[] expectedCounts, Direction direction )
    {
        int result = 0;
        if ( direction == Direction.OUTGOING || direction == Direction.BOTH )
        {
            result += expectedCounts[0];
        }
        if ( direction == Direction.INCOMING || direction == Direction.BOTH )
        {
            result += expectedCounts[1];
        }
        result += expectedCounts[2];
        return result;
    }

    private void deleteOneRelationship( Node node, RelType type, Direction direction, int which )
    {
        Relationship last = null;
        int counter = 0;
        Iterable<Relationship> relationships = node.getRelationships( direction, type );
        try ( ResourceIterator<Relationship> relationshipIterator = (ResourceIterator) relationships.iterator() )
        {
            while ( relationshipIterator.hasNext() )
            {
                Relationship rel = relationshipIterator.next();
                if ( isLoop( rel ) == (direction == Direction.BOTH) )
                {
                    last = rel;
                    if ( counter++ == which )
                    {
                        rel.delete();
                        return;
                    }
                }
            }
        }
        if ( which == Integer.MAX_VALUE && last != null )
        {
            last.delete();
            return;
        }

        fail( "Couldn't find " + (direction == Direction.BOTH ? "loop" : "non-loop") + " relationship " +
                type.name() + " " + direction + " to delete" );
    }

    private boolean isLoop( Relationship r )
    {
        return r.getStartNode().equals( r.getEndNode() );
    }

    private static class RelationshipCreationSpec
    {
        private final RelType type;
        private final Direction dir;
        private final int count;

        RelationshipCreationSpec( RelType type, Direction dir, int count )
        {
            this.type = type;
            this.dir = dir;
            this.count = count;
        }
    }

    private static class RelationshipDeletionSpec
    {
        private final RelType type;
        private final Direction dir;
        private final int which;

        RelationshipDeletionSpec( RelType type, Direction dir, int which /*Integer.MAX_VALUE==last*/ )
        {
            this.type = type;
            this.dir = dir;
            this.which = which;
        }
    }

    static RelationshipCreationSpec create( RelType type, Direction dir, int count )
    {
        return new RelationshipCreationSpec( type, dir, count );
    }

    static RelationshipDeletionSpec delete( RelType type, Direction dir, int which )
    {
        return new RelationshipDeletionSpec( type, dir, which );
    }

    private enum RelType implements RelationshipType
    {
        INITIAL( false ),
        TYPE1( true ),
        TYPE2( true );

        boolean measure;

        RelType( boolean measure )
        {
            this.measure = measure;
        }
    }
}

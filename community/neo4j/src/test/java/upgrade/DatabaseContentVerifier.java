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
package upgrade;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.makeLongArray;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.makeLongString;

public class DatabaseContentVerifier
{
    private final String longString = makeLongString();
    private final int[] longArray = makeLongArray();
    private final GraphDatabaseService database;
    private final int numberOfUnrelatedNodes;

    public DatabaseContentVerifier( GraphDatabaseService database, int numberOfUnrelatedNodes )
    {
        this.database = database;
        this.numberOfUnrelatedNodes = numberOfUnrelatedNodes;
    }

    public void verifyRelationships( int expectedCount )
    {
        try ( Transaction tx = database.beginTx() )
        {
            int traversalCount = 0;
            for ( Relationship rel : GlobalGraphOperations.at( database ).getAllRelationships() )
            {
                traversalCount++;
                verifyProperties( rel );
            }
            tx.success();
            assertEquals( expectedCount, traversalCount );
        }
    }

    public void verifyNodes( int expectedCount )
    {
        int nodeCount = 0;
        try ( Transaction tx = database.beginTx() )
        {
            for ( Node node : GlobalGraphOperations.at( database ).getAllNodes() )
            {
                nodeCount++;
                if ( node.getId() >= numberOfUnrelatedNodes )
                {
                    verifyProperties( node );
                }
            }
            tx.success();
        }
        assertEquals( expectedCount, nodeCount );
    }

    public void verifyProperties( PropertyContainer node )
    {
        try
        {
            assertEquals( Integer.MAX_VALUE, node.getProperty( PropertyType.INT.name() ) );
            assertEquals( longString, node.getProperty( PropertyType.STRING.name() ) );
            assertEquals( true, node.getProperty( PropertyType.BOOL.name() ) );
            assertEquals( Double.MAX_VALUE, node.getProperty( PropertyType.DOUBLE.name() ) );
            assertEquals( Float.MAX_VALUE, node.getProperty( PropertyType.FLOAT.name() ) );
            assertEquals( Long.MAX_VALUE, node.getProperty( PropertyType.LONG.name() ) );
            assertEquals( Byte.MAX_VALUE, node.getProperty( PropertyType.BYTE.name() ) );
            assertEquals( Character.MAX_VALUE, node.getProperty( PropertyType.CHAR.name() ) );
            assertArrayEquals( longArray, (int[]) node.getProperty( PropertyType.ARRAY.name() ) );
            assertEquals( Short.MAX_VALUE, node.getProperty( PropertyType.SHORT.name() ) );
            assertEquals( "short", node.getProperty( PropertyType.SHORT_STRING.name() ) );
        }
        catch ( NotFoundException e )
        {
            throw new NotFoundException( e.getMessage() + " for " + node, e.getCause() );
        }
    }

    public void verifyNodeIdsReused()
    {
        try ( Transaction transaction = database.beginTx() )
        {
            Node newNode = database.createNode();
            assertThat( newNode.getId(), lessThanOrEqualTo( 10L ) );
            transaction.success();
        }
    }

    public void verifyRelationshipIdsReused()
    {
        try ( Transaction transaction = database.beginTx() )
        {
            Node node1 = database.createNode();
            Node node2 = database.createNode();
            Relationship relationship1 = node1.createRelationshipTo( node2, withName( "REUSE" ) );
            assertEquals( 0, relationship1.getId() );
            transaction.success();
        }
    }

    public void verifyIndex()
    {
        try ( Transaction tx = database.beginTx() )
        {
            List<IndexDefinition> indexDefinitions = Iterables.toList( database.schema().getIndexes() );
            assertEquals( 1, indexDefinitions.size() );
            IndexDefinition indexDefinition = indexDefinitions.get( 0 );
            assertEquals( "Label", indexDefinition.getLabel().name() );
            List<String> propKeys = Iterables.toList( indexDefinition.getPropertyKeys() );
            assertEquals( 1, propKeys.size() );
            String propKey = propKeys.get( 0 );
            assertEquals( "prop", propKey );
            tx.success();
        }
    }

    public void verifyLegacyIndex()
    {
        try ( Transaction tx = database.beginTx() )
        {
            String[] nodeIndexes = database.index().nodeIndexNames();
            String[] relationshipIndexes = database.index().relationshipIndexNames();
            assertEquals( asSet( "testIndex", "nodekey" ), asSet( nodeIndexes ) );
            assertEquals( asSet( "relkey" ), asSet( relationshipIndexes ) );
            tx.success();
        }
    }

    public void verifyJohnnyLabels()
    {
        // Johnny labels has got a bunch of alter egos
        try ( Transaction tx = database.beginTx() )
        {
            Node johhnyLabels = database.getNodeById( 1 );
            Set<String> expectedLabels = new HashSet<>();
            for ( int i = 0; i < 30; i++ )
            {
                expectedLabels.add( "AlterEgo" + i );
            }
            for ( Label label : johhnyLabels.getLabels() )
            {
                assertTrue( expectedLabels.remove( label.name() ) );
            }
            assertTrue( expectedLabels.isEmpty() );
            tx.success();
        }
    }
}

/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.makeLongArray;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.makeLongString;

public class DatabaseContentVerifier
{
    private final String longString = makeLongString();
    private final int[] longArray = makeLongArray();
    private final GraphDatabaseService database;

    public DatabaseContentVerifier( GraphDatabaseService database )
    {
        this.database = database;
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
                if ( node.getId() > 0 )
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

    public void verifyNodeIdsReused()
    {
        try ( Transaction ignore = database.beginTx() )
        {
            database.getNodeById( 1 );
            fail( "Node 1 should not exist" );
        }
        catch ( NotFoundException e )
        {   // expected
        }

        try ( Transaction transaction = database.beginTx() )
        {
            Node newNode = database.createNode();
            assertEquals( 1, newNode.getId() );
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
            assertArrayEquals( new String[]{"nodekey"}, nodeIndexes );
            assertArrayEquals( new String[]{"relkey"}, relationshipIndexes );
            tx.success();
        }
    }
}

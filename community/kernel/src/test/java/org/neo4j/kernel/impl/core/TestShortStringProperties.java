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
package org.neo4j.kernel.impl.core;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.lang.reflect.Field;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.TestShortString;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.GraphTransactionRule;
import org.neo4j.test.ImpermanentDatabaseRule;

import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.graphdb.Neo4jMatchers.hasProperty;
import static org.neo4j.graphdb.Neo4jMatchers.inTx;

public class TestShortStringProperties extends TestShortString
{
    @ClassRule
    public static DatabaseRule graphdb = new ImpermanentDatabaseRule();

    @Rule
    public GraphTransactionRule tx = new GraphTransactionRule( graphdb );

    public void commit()
    {
        tx.success();
    }

    public void newTx()
    {
        tx.success();
        tx.begin();
    }

    private static final String LONG_STRING = "this is a really long string, believe me!";

    @Test
    public void canAddMultipleShortStringsToTheSameNode() throws Exception
    {
        long recordCount = dynamicRecordsInUse();
        Node node = graphdb.getGraphDatabaseService().createNode();
        node.setProperty( "key", "value" );
        node.setProperty( "reverse", "esrever" );
        commit();
        assertEquals( recordCount, dynamicRecordsInUse() );
        assertThat( node, inTx( graphdb.getGraphDatabaseService(), hasProperty( "key" ).withValue( "value" )  ) );
        assertThat( node, inTx( graphdb.getGraphDatabaseService(), hasProperty( "reverse" ).withValue( "esrever" )  ) );
    }

    @Test
    public void canAddShortStringToRelationship() throws Exception
    {
        long recordCount = dynamicRecordsInUse();
        GraphDatabaseService db = graphdb.getGraphDatabaseService();
        Relationship rel = db.createNode().createRelationshipTo( db.createNode(), withName( "REL_TYPE" ) );
        rel.setProperty( "type", "dimsedut" );
        commit();
        assertEquals( recordCount, dynamicRecordsInUse() );
        assertThat( rel, inTx( db, hasProperty( "type" ).withValue( "dimsedut" ) ) );
    }

    @Test
    public void canUpdateShortStringInplace() throws Exception
    {
        try
        {
            long recordCount = dynamicRecordsInUse();
            long propCount = propertyRecordsInUse();
            Node node = graphdb.getGraphDatabaseService().createNode();
            node.setProperty( "key", "value" );

            newTx();

            assertEquals( recordCount, dynamicRecordsInUse() );
            assertEquals( propCount + 1, propertyRecordsInUse() );
            assertEquals( "value", node.getProperty( "key" ) );

            node.setProperty( "key", "other" );
            commit();

            assertEquals( recordCount, dynamicRecordsInUse() );
            assertEquals( propCount + 1, propertyRecordsInUse() );
            assertThat( node, inTx( graphdb.getGraphDatabaseService(), hasProperty( "key" ).withValue( "other" )  ) );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void canReplaceLongStringWithShortString() throws Exception
    {
        long recordCount = dynamicRecordsInUse();
        long propCount = propertyRecordsInUse();
        Node node = graphdb.getGraphDatabaseService().createNode();
        node.setProperty( "key", LONG_STRING );
        newTx();

        assertEquals( recordCount + 1, dynamicRecordsInUse() );
        assertEquals( propCount + 1, propertyRecordsInUse() );
        assertEquals( LONG_STRING, node.getProperty( "key" ) );

        node.setProperty( "key", "value" );
        commit();

        assertEquals( recordCount, dynamicRecordsInUse() );
        assertEquals( propCount + 1, propertyRecordsInUse() );
        assertThat( node, inTx( graphdb.getGraphDatabaseService(), hasProperty( "key" ).withValue( "value" )  ) );
    }

    @Test
    public void canReplaceShortStringWithLongString() throws Exception
    {
        long recordCount = dynamicRecordsInUse();
        long propCount = propertyRecordsInUse();
        Node node = graphdb.getGraphDatabaseService().createNode();
        node.setProperty( "key", "value" );
        newTx();

        assertEquals( recordCount, dynamicRecordsInUse() );
        assertEquals( propCount + 1, propertyRecordsInUse() );
        assertEquals( "value", node.getProperty( "key" ) );

        node.setProperty( "key", LONG_STRING );
        commit();

        assertEquals( recordCount + 1, dynamicRecordsInUse() );
        assertEquals( propCount + 1, propertyRecordsInUse() );
        assertThat( node, inTx( graphdb.getGraphDatabaseService(), hasProperty( "key" ).withValue( LONG_STRING )  ) );
    }

    @Test
    public void canRemoveShortStringProperty() throws Exception
    {
        long recordCount = dynamicRecordsInUse();
        long propCount = propertyRecordsInUse();
        GraphDatabaseService db = graphdb.getGraphDatabaseService();
        Node node = db.createNode();
        node.setProperty( "key", "value" );
        newTx();

        assertEquals( recordCount, dynamicRecordsInUse() );
        assertEquals( propCount + 1, propertyRecordsInUse() );
        assertEquals( "value", node.getProperty( "key" ) );

        node.removeProperty( "key" );
        commit();

        assertEquals( recordCount, dynamicRecordsInUse() );
        assertEquals( propCount, propertyRecordsInUse() );
        assertThat( node, inTx( db, not( hasProperty( "key" ) ) ) );
    }

    // === reuse the test cases from the encoding ===

    @Override
    protected void assertCanEncode( String string )
    {
        encode( string, true );
    }

    private void encode( String string, boolean isShort )
    {
        long recordCount = dynamicRecordsInUse();
        Node node = graphdb.getGraphDatabaseService().createNode();
        node.setProperty( "key", string );
        newTx();
        if ( isShort )
        {
            assertEquals( recordCount, dynamicRecordsInUse() );
        }
        else
        {
            assertTrue( recordCount < dynamicRecordsInUse() );
        }
        assertEquals( string, node.getProperty( "key" ) );
    }

    // === Here be (reflection) dragons ===

    private static Field storeField;
    static
    {
        try
        {
            storeField = PropertyStore.class.getDeclaredField( "stringPropertyStore" );
            storeField.setAccessible( true );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    private long propertyRecordsInUse()
    {
        return AbstractNeo4jTestCase.numberOfRecordsInUse( propertyStore() );
    }

    private long dynamicRecordsInUse()
    {
        return AbstractNeo4jTestCase.numberOfRecordsInUse( propertyStore().getStringStore() );
    }

    private PropertyStore propertyStore()
    {
        return graphdb.getGraphDatabaseAPI().getDependencyResolver().resolveDependency( NeoStoreDataSource.class).getNeoStores().getPropertyStore();
    }
}

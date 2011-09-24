/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;

import java.lang.reflect.Field;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.nioneo.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.TestShortString;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaConnection;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.test.TargetDirectory;

public class TestShortStringProperties extends TestShortString
{
    private static final TargetDirectory target = TargetDirectory.forTest( TestShortStringProperties.class );
    private static AbstractGraphDatabase graphdb;

    @BeforeClass
    public static void startup()
    {
        graphdb = new EmbeddedGraphDatabase( target.graphDbDir( true ).getAbsolutePath() );
    }

    @AfterClass
    public static void shutdown()
    {
        if ( graphdb != null ) graphdb.shutdown();
        graphdb = null;
    }

    private Transaction tx;

    @Before
    public void beginTx()
    {
        if ( tx == null ) tx = graphdb.beginTx();
    }

    @After
    public void finishTx()
    {
        if ( tx != null ) tx.finish();
        tx = null;
    }

    public void commit()
    {
        if ( tx != null ) tx.success();
        finishTx();
        clearCache();
    }

    public void newTx()
    {
        commit();
        beginTx();
    }

    private void clearCache()
    {
        graphdb.getConfig().getGraphDbModule().getNodeManager().clearCache();
    }

    private static final String LONG_STRING = "this is a really long string, believe me!";

    @Test
    public void canAddMultipleShortStringsToTheSameNode() throws Exception
    {
        long recordCount = dynamicRecordsInUse();
        Node node = graphdb.createNode();
        node.setProperty( "key", "value" );
        node.setProperty( "reverse", "esrever" );
        commit();
        assertEquals( recordCount, dynamicRecordsInUse() );
        assertEquals( "value", node.getProperty( "key" ) );
        assertEquals( "esrever", node.getProperty( "reverse" ) );
    }

    @Test
    public void canAddShortStringToRelationship() throws Exception
    {
        long recordCount = dynamicRecordsInUse();
        Relationship rel = graphdb.createNode().createRelationshipTo( graphdb.createNode(), withName( "REL_TYPE" ) );
        rel.setProperty( "type", rel.getType().name() );
        commit();
        assertEquals( recordCount, dynamicRecordsInUse() );
        assertEquals( rel.getType().name(), rel.getProperty( "type" ) );
    }

    @Test
    public void canUpdateShortStringInplace() throws Exception
    {
        long recordCount = dynamicRecordsInUse();
        long propCount = propertyRecordsInUse();
        Node node = graphdb.createNode();
        node.setProperty( "key", "value" );

        newTx();

        assertEquals( recordCount, dynamicRecordsInUse() );
        assertEquals( propCount + 1, propertyRecordsInUse() );
        assertEquals( "value", node.getProperty( "key" ) );

        node.setProperty( "key", "other" );
        commit();

        assertEquals( recordCount, dynamicRecordsInUse() );
        assertEquals( propCount + 1, propertyRecordsInUse() );
        assertEquals( "other", node.getProperty( "key" ) );
    }

    @Test
    public void canReplaceLongStringWithShortString() throws Exception
    {
        long recordCount = dynamicRecordsInUse();
        long propCount = propertyRecordsInUse();
        Node node = graphdb.createNode();
        node.setProperty( "key", LONG_STRING );
        newTx();

        assertEquals( recordCount + 1, dynamicRecordsInUse() );
        assertEquals( propCount + 1, propertyRecordsInUse() );
        assertEquals( LONG_STRING, node.getProperty( "key" ) );

        node.setProperty( "key", "value" );
        commit();

        assertEquals( recordCount, dynamicRecordsInUse() );
        assertEquals( propCount + 1, propertyRecordsInUse() );
        assertEquals( "value", node.getProperty( "key" ) );
    }

    @Test
    public void canReplaceShortStringWithLongString() throws Exception
    {
        long recordCount = dynamicRecordsInUse();
        long propCount = propertyRecordsInUse();
        Node node = graphdb.createNode();
        node.setProperty( "key", "value" );
        newTx();

        assertEquals( recordCount, dynamicRecordsInUse() );
        assertEquals( propCount + 1, propertyRecordsInUse() );
        assertEquals( "value", node.getProperty( "key" ) );

        node.setProperty( "key", LONG_STRING );
        commit();

        assertEquals( recordCount + 1, dynamicRecordsInUse() );
        assertEquals( propCount + 1, propertyRecordsInUse() );
        assertEquals( LONG_STRING, node.getProperty( "key" ) );
    }

    @Test
    public void canRemoveShortStringProperty() throws Exception
    {
        long recordCount = dynamicRecordsInUse();
        long propCount = propertyRecordsInUse();
        Node node = graphdb.createNode();
        node.setProperty( "key", "value" );
        newTx();

        assertEquals( recordCount, dynamicRecordsInUse() );
        assertEquals( propCount + 1, propertyRecordsInUse() );
        assertEquals( "value", node.getProperty( "key" ) );

        node.removeProperty( "key" );
        commit();

        assertEquals( recordCount, dynamicRecordsInUse() );
        assertEquals( propCount, propertyRecordsInUse() );
        assertFalse( node.hasProperty( "key" ) );
    }

    // === reuse the test cases from the encoding ===

    @Override
    protected void assertCanEncode( String string )
    {
        encode( string, true );
    }

    @Override
    protected void assertCannotEncode( String string )
    {
        encode( string, false );
    }

    private void encode( String string, boolean isShort )
    {
        long recordCount = dynamicRecordsInUse();
        Node node = graphdb.createNode();
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
        clearCache();
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
        return propertyStore().getNumberOfIdsInUse();
    }

    private long dynamicRecordsInUse()
    {
        try
        {
            return ( (AbstractDynamicStore) storeField.get( propertyStore() ) ).getNumberOfIdsInUse();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    private PropertyStore propertyStore()
    {
        XaDataSourceManager dsMgr = graphdb.getConfig().getTxModule().getXaDataSourceManager();
        return ( (NeoStoreXaConnection) dsMgr.getXaDataSource( Config.DEFAULT_DATA_SOURCE_NAME ).getXaConnection() ).getPropertyStore();
    }
}

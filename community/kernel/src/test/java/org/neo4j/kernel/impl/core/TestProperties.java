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
package org.neo4j.kernel.impl.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.IteratorUtil.first;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.test.TargetDirectory;

public class TestProperties extends AbstractNeo4jTestCase
{
    @Test
    public void addAndRemovePropertiesWithinOneTransaction() throws Exception
    {
        Node node = getGraphDb().createNode();

        node.setProperty( "name", "oscar" );
        node.setProperty( "favourite_numbers", new Long[] { 1l, 2l, 3l } );
        node.setProperty( "favourite_colors", new String[] { "blue", "red" } );
        node.removeProperty( "favourite_colors" );
        newTransaction();

        assertNotNull( node.getProperty( "favourite_numbers", null ) );
    }

    @Test
    public void addAndRemovePropertiesWithinOneTransaction2() throws Exception
    {
        Node node = getGraphDb().createNode();
        node.setProperty( "foo", "bar" );

        newTransaction();
        node.setProperty( "foo2", "bar" );
        node.removeProperty( "foo" );

        newTransaction();

        try
        {
            node.getProperty( "foo" );
            fail( "property should not exist" );
        }
        catch ( NotFoundException e )
        {
            // good
        }
    }

    @Test
    public void setPropertyWithEmptyStringAsName() throws Exception
    {
        /*
         * Tries to create a property with an empty string as a property key
         * and then tries to read it back. Uses a separate EmbeddedGraphDatabase
         * with a TargetDirectory because we have to restart to test hitting the
         * PropertyIndexStore on disk when reading it back.
         */
        String path = TargetDirectory.forTest( TestProperties.class ).directory(
                "empty-string", true ).getCanonicalPath();
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( path );
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        long nodeId = node.getId();
        node.setProperty( "", "bar" );
        tx.success();
        tx.finish();
        assertEquals( "bar", db.getNodeById( nodeId ).getProperty( "" ) );
        db.shutdown();
        db = new GraphDatabaseFactory().newEmbeddedDatabase( path );
        assertEquals( "bar", db.getNodeById( nodeId ).getProperty( "" ) );
    }

    @Test
    public void removeAndAddSameProperty() throws Exception
    {
        Node node = getGraphDb().createNode();
        node.setProperty( "foo", "bar" );
        newTransaction();

        node.removeProperty( "foo" );
        node.setProperty( "foo", "bar" );
        newTransaction();
        assertEquals( "bar", node.getProperty( "foo" ) );

        node.setProperty( "foo", "bar" );
        node.removeProperty( "foo" );
        newTransaction();
        assertNull( node.getProperty( "foo", null ) );
    }

    @Test
    public void removeSomeAndSetSome() throws Exception
    {
        Node node = getGraphDb().createNode();
        node.setProperty( "remove me", "trash" );
        newTransaction();

        node.removeProperty( "remove me" );
        node.setProperty( "foo", "bar" );
        node.setProperty( "baz", 17 );
        newTransaction();

        assertEquals( "bar", node.getProperty( "foo" ) );
        assertEquals( 17, node.getProperty( "baz" ) );
        assertNull( node.getProperty( "remove me", null ) );
    }

    @Test
    public void removeOneOfThree() throws Exception
    {
        Node node = getGraphDb().createNode();
        node.setProperty( "1", 1 );
        node.setProperty( "2", 2 );
        node.setProperty( "3", 3 );
        newTransaction();

        node.removeProperty( "2" );
        newTransaction();
        assertNull( node.getProperty( "2", null ) );
    }

    @Test
    public void testLongPropertyValues() throws Exception
    {
        Node n = getGraphDb().createNode();
        setPropertyAndAssertIt( n, -134217728L );
        setPropertyAndAssertIt( n, -134217729L );
    }

    @Test
    public void testIntPropertyValues() throws Exception
    {
        Node n = getGraphDb().createNode();
        setPropertyAndAssertIt( n, -134217728 );
        setPropertyAndAssertIt( n, -134217729 );
    }

    @Test
    public void booleanRange() throws Exception
    {
        Node node = getGraphDb().createNode();
        setPropertyAndAssertIt( node, false );
        setPropertyAndAssertIt( node, true );
    }

    @Test
    public void byteRange() throws Exception
    {
        Node node = getGraphDb().createNode();
        for ( byte i = Byte.MIN_VALUE; i < Byte.MAX_VALUE; i++ )
        {
            setPropertyAndAssertIt( node, i );
        }
    }

    @Test
    public void charRange() throws Exception
    {
        Node node = getGraphDb().createNode();
        for ( char i = Character.MIN_VALUE; i < Character.MAX_VALUE; i++ )
        {
            setPropertyAndAssertIt( node, i );
        }
    }

    @Test
    public void shortRange() throws Exception
    {
        Node node = getGraphDb().createNode();
        for ( short i = Short.MIN_VALUE; i < Short.MAX_VALUE; i++ )
        {
            setPropertyAndAssertIt( node, i );
        }
    }

    @Test
    public void intRange() throws Exception
    {
        int step = 30001;
        Node node = getGraphDb().createNode();
        for ( int i = Integer.MIN_VALUE; i < Integer.MAX_VALUE-step; i += step )
        {
            setPropertyAndAssertIt( node, i );
        }
    }

    @Test
    public void longRange() throws Exception
    {
        long step = 120000000000001L;
        Node node = getGraphDb().createNode();
        for ( long i = Long.MIN_VALUE; i < Long.MAX_VALUE-step; i += step )
        {
            setPropertyAndAssertIt( node, i );
        }
    }

    @Test
    public void floatRange() throws Exception
    {
        float step = 1234567890123456789012345678901234.1234F;
        Node node = getGraphDb().createNode();
        for ( float i = Float.MIN_VALUE; i < Float.MAX_VALUE-step; i += step )
        {
            setPropertyAndAssertIt( node, i );
        }
    }

    @Test
    public void doubleRange() throws Exception
    {
        double step = 12.345;
        Node node = getGraphDb().createNode();
        for ( double i = Double.MIN_VALUE; i < Double.MAX_VALUE; i += step, step *= 1.004D )
        {
            setPropertyAndAssertIt( node, i );
        }
    }

    private void setPropertyAndAssertIt( Node node, Object value )
    {
        node.setProperty( "key", value );
        assertEquals( value, node.getProperty( "key" ) );
    }
    
    @Test
    public void loadManyProperties() throws Exception
    {
        Node node = getGraphDb().createNode();
        for ( int i = 0; i < 1000; i++ )
        {
            node.setProperty( "property " + i, "value" );
        }
        newTransaction();
        clearCache();
        assertEquals( "value", node.getProperty( "property 0" ) );
    }

    @Test
    public void getPropertyValuesShouldReturnCorrectValues() throws Exception
    {
        String key = "title";
        // value1 fits in an inlined record
        String value1 = "123456789123456789123456789123456789123456789123456789";
        // value2 requires a dynamic record
        String value2 = "12345678912345678912345678912345678912345678912345678912";
        Node node1 = getGraphDb().createNode();
        node1.setProperty( key, value1 );
        Node node2 = getGraphDb().createNode();
        node2.setProperty( key, value2 );
        newTransaction();
        clearCache();
        
        assertEquals( value1, first( node1.getPropertyValues() ) );
        assertEquals( value2, first( node2.getPropertyValues() ) );
    }
    
    @Test
    public void name() throws Exception
    {
        Node node = getGraphDb().createNode();
        node.setProperty( "name", "yo" );
        node.getProperty( "name" );
        commit();
        node.getProperty( "name" );
    }
}

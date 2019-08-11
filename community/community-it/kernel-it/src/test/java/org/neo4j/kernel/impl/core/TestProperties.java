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
package org.neo4j.kernel.impl.core;

import org.junit.jupiter.api.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestProperties extends AbstractNeo4jTestCase
{
    private static final int VALUE_RANGE_SPLIT = 20;

    @Test
    void addAndRemovePropertiesWithinOneTransaction()
    {
        Node node = createNode();

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.setProperty( "name", "oscar" );
            node.setProperty( "favourite_numbers", new Long[] { 1L, 2L, 3L } );
            node.setProperty( "favourite_colors", new String[] { "blue", "red" } );
            node.removeProperty( "favourite_colors" );
            transaction.commit();
        }

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            assertNotNull( node.getProperty( "favourite_numbers", null ) );
        }
    }

    @Test
    void addAndRemovePropertiesWithinOneTransaction2()
    {
        Node node = createNode();

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.setProperty( "foo", "bar" );
            transaction.commit();
        }

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.setProperty( "foo2", "bar" );
            node.removeProperty( "foo" );
            transaction.commit();
        }

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            assertThrows( NotFoundException.class, () -> node.getProperty( "foo" ) );
        }
    }

    @Test
    void removeAndAddSameProperty()
    {
        Node node = createNode();
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.setProperty( "foo", "bar" );
            transaction.commit();
        }

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.removeProperty( "foo" );
            node.setProperty( "foo", "bar" );
            transaction.commit();
        }
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            assertEquals( "bar", node.getProperty( "foo" ) );
            transaction.commit();
        }

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.setProperty( "foo", "bar" );
            node.removeProperty( "foo" );
            transaction.commit();
        }

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            assertNull( node.getProperty( "foo", null ) );
        }
    }

    @Test
    void removeSomeAndSetSome()
    {
        Node node = createNode();
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.setProperty( "remove me", "trash" );
            transaction.commit();
        }
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.removeProperty( "remove me" );
            node.setProperty( "foo", "bar" );
            node.setProperty( "baz", 17 );
            transaction.commit();
        }

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            assertEquals( "bar", node.getProperty( "foo" ) );
            assertEquals( 17, node.getProperty( "baz" ) );
            assertNull( node.getProperty( "remove me", null ) );
            transaction.commit();
        }
    }

    @Test
    void removeOneOfThree()
    {
        Node node = createNode();
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.setProperty( "1", 1 );
            node.setProperty( "2", 2 );
            node.setProperty( "3", 3 );
            transaction.commit();
        }

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.removeProperty( "2" );
            transaction.commit();
        }
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            assertNull( node.getProperty( "2", null ) );
            transaction.commit();
        }
    }

    @Test
    void testLongPropertyValues()
    {
        Node n = createNode();
        setPropertyAndAssertIt( n, -134217728L );
        setPropertyAndAssertIt( n, -134217729L );
    }

    @Test
    void testIntPropertyValues()
    {
        Node n = createNode();
        setPropertyAndAssertIt( n, -134217728 );
        setPropertyAndAssertIt( n, -134217729 );
    }

    @Test
    void booleanRange()
    {
        Node node = createNode();
        setPropertyAndAssertIt( node, false );
        setPropertyAndAssertIt( node, true );
    }

    @Test
    void byteRange()
    {
        Node node = createNode();
        byte stride = Byte.MAX_VALUE / VALUE_RANGE_SPLIT;
        for ( byte i = Byte.MIN_VALUE; i < Byte.MAX_VALUE; )
        {
            setPropertyAndAssertIt( node, i );
            i = i > 0 && Byte.MAX_VALUE - i < stride ? Byte.MAX_VALUE : (byte) (i + stride);
        }
    }

    @Test
    void charRange()
    {
        Node node = createNode();
        char stride = Character.MAX_VALUE / VALUE_RANGE_SPLIT;
        for ( char i = Character.MIN_VALUE; i < Character.MAX_VALUE; )
        {
            setPropertyAndAssertIt( node, i );
            i = i > 0 && Character.MAX_VALUE - i < stride ? Character.MAX_VALUE : (char) (i + stride);
        }
    }

    @Test
    void shortRange()
    {
        Node node = createNode();
        short stride = Short.MAX_VALUE / VALUE_RANGE_SPLIT;
        for ( short i = Short.MIN_VALUE; i < Short.MAX_VALUE; )
        {
            setPropertyAndAssertIt( node, i );
            i = i > 0 && Short.MAX_VALUE - i < stride ? Short.MAX_VALUE : (short) (i + stride);
        }
    }

    @Test
    void intRange()
    {
        Node node = createNode();
        int stride = Integer.MAX_VALUE / VALUE_RANGE_SPLIT;
        for ( int i = Integer.MIN_VALUE; i < Integer.MAX_VALUE; )
        {
            setPropertyAndAssertIt( node, i );
            i = i > 0 && Integer.MAX_VALUE - i < stride ? Integer.MAX_VALUE : i + stride;
        }
    }

    @Test
    void longRange()
    {
        Node node = createNode();
        long stride = Long.MAX_VALUE / VALUE_RANGE_SPLIT;
        for ( long i = Long.MIN_VALUE; i < Long.MAX_VALUE; )
        {
            setPropertyAndAssertIt( node, i );
            i = i > 0 && Long.MAX_VALUE - i < stride ? Long.MAX_VALUE : i + stride;
        }
    }

    @Test
    void floatRange()
    {
        Node node = createNode();
        float stride = 16f;
        for ( float i = Float.MIN_VALUE; i < Float.MAX_VALUE; )
        {
            setPropertyAndAssertIt( node, i );
            setPropertyAndAssertIt( node, -i );
            i *= stride;
        }
    }

    @Test
    void doubleRange()
    {
        Node node = createNode();
        double stride = 4194304d; // 2^23
        for ( double i = Double.MIN_VALUE; i < Double.MAX_VALUE; )
        {
            setPropertyAndAssertIt( node, i );
            setPropertyAndAssertIt( node, -i );
            i *= stride;
        }
    }

    private void setPropertyAndAssertIt( Node node, Object value )
    {
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.setProperty( "key", value );
            assertEquals( value, node.getProperty( "key" ) );
            transaction.commit();
        }
    }

    @Test
    void loadManyProperties()
    {
        Node node = createNode();
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            for ( int i = 0; i < 200; i++ )
            {
                node.setProperty( "property " + i, "value" );
            }
            transaction.commit();
        }
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            assertEquals( "value", node.getProperty( "property 0" ) );
            transaction.commit();
        }
    }

    @Test
    void name()
    {
        Node node = createNode();
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.setProperty( "name", "yo" );
            node.getProperty( "name" );
            transaction.commit();
        }

        try ( Transaction tx = getGraphDb().beginTx() )
        {
            node.getProperty( "name" );
            tx.commit();
        }
    }
}

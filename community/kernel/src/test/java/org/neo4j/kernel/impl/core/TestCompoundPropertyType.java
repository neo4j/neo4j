/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;

public class TestCompoundPropertyType extends AbstractNeo4jTestCase
{
    private Node node1 = null;

    @Before
    public void createInitialNode()
    {
        node1 = getGraphDb().createNode();
    }

    @After
    public void deleteInitialNode()
    {
        node1.delete();
    }

    @Test
    public void testCompound()
    {
        String key = "testing";

        Map<String, Object> m = newMap();
        m.put( "pi", 31415 );
        m.put( "zero", 0.0 );

        node1.setProperty( key, m );

        newTransaction();
        clearCache();

        assertMapEquals( m, (Map<String, Object>) node1.getProperty( key ) );
    }

    @Test
    public void testCompoundNested()
    {
        String key = "testing";

        Map<String, Object> m = newMap();
        m.put( "pi", 31415 );
        m.put( "zero", 0.0 );

        Map<String, Object> nested1 = newMap();
        nested1.put( "n1", "n1" );
        m.put( "nested1", nested1 );

        Map<String, Object> nested2 = Collections.singletonMap( "n2", (Object)"n2" );
        m.put( "nested2", nested2 );

        node1.setProperty( key, m );

        newTransaction();
        clearCache();

        assertMapEquals( m, (Map<String, Object>) node1.getProperty( key ) );
    }

    @Test
    public void testCompoundCycle()
    {
        Map<String, Object> m = newMap();
        m.put( "some", "value" );
        m.put( "cycle", m );

        try
        {
            node1.setProperty( "cycle1", m );
            fail( "Property setting should fail due to cycle detection" );
        }
        catch( IllegalArgumentException ex )
        {
        }

        m = newMap();
        Map<String, Object> nested = newMap();
        m.put( "nested", nested );

        nested.put( "key", "value" );
        nested.put( "m", m );

        try
        {
            node1.setProperty( "cycle2", m );
            fail( "Property setting should fail due to cycle detection" );
        }
        catch( IllegalArgumentException ex )
        {
        }
    }

    @Test( expected = IllegalArgumentException.class )
    public void testCompoundNullKey()
    {
        Map<String, Object> m = newMap();
        m.put( null, "nullKey" );

        node1.setProperty( "map", m );
        fail( "Property setting should fail due to null key" );
    }

    @Test
    public void testCompoundNullValue()
    {
        Map<String, Object> m = newMap();
        m.put( "key", "value" );
        m.put( "nullValue", null );

        node1.setProperty( "map", m );
        newTransaction();

        m = (Map<String, Object>)node1.getProperty( "map" );
        assertEquals( "Null value not expected", 1, m.size() );
        assertEquals( "value", m.get( "key" ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void testCompoundNullKeyAndValue()
    {
        Map<String, Object> m = newMap();
        m.put( "key", "value" );
        m.put( null, null );

        node1.setProperty( "map", m );
        fail( "Property setting should fail due to null key" );
    }

    @Test
    public void testEmptyCompound()
    {
        Map<String, Object> m = newMap();

        node1.setProperty( "empty", m );

        newTransaction();
        clearCache();

        assertMapEquals( m, (Map<String, Object>) node1.getProperty( "empty" ) );
    }

    @Test
    public void testEmptyNestedCompound()
    {
        Map<String, Object> current = newMap();
        for ( int i = 0 ; i < 10 ; i++ )
        {
            Map<String, Object> nested = newMap();
            nested.put( "level" + i, current );
            current = nested;
        }
        node1.setProperty( "deep empty", current );

        newTransaction();
        clearCache();

        assertMapEquals( current, (Map<String, Object>) node1.getProperty( "deep empty" ) );
    }

    @Test
    public void removeAndAddSameCompound() throws Exception
    {
        Node node = getGraphDb().createNode();
        Map<String, Object> m = newMap();
        m.put( "foo", "bar" );

        node.setProperty( "m", m );
        newTransaction();

        node.removeProperty( "m" );
        node.setProperty( "m", m );
        newTransaction();
        assertEquals( m, node.getProperty( "m" ) );

        node.setProperty( "m", m );
        node.removeProperty( "m" );
        newTransaction();
        assertNull( node.getProperty( "m", null ) );
    }

    @Test
    public void testLargeCompound()
    {
        String key = "testing";

        Map<String, Object> m = newMap();
        for ( int i = 0 ; i < 1000 ; i++ )
        {
            m.put( "key" + i, largeString() + i );
        }

        node1.setProperty( key, m );

        newTransaction();
        clearCache();

        assertMapEquals( m, (Map<String, Object>) node1.getProperty( key ) );
    }

    @Test
    public void testLargeNestedCompound()
    {
        String key = "testing";

        Map<String, Object> m = newMap();
        for ( int i = 0 ; i < 100 ; i++ )
        {
            m.put( "string key " + i, largeString() + i );

            Map<String, Object> nested = newMap();
            m.put( "map " + i, nested );

            for ( int j = 0 ; j < 10 ; j++ )
            {
                Map<String, Object> n = newMap();
                n.put( "nested string key "+ i + "/" + j , i + "/" + j );

                nested.put( "level " + j, n );
                nested = n;
            }
        }
        node1.setProperty( key, m );

        newTransaction();
        clearCache();

        assertMapEquals( m, (Map<String, Object>) node1.getProperty( key ) );
    }

    @Test
    public void testMultipleCompounds() {
        String key1 = "testing1";

        Map<String, Object> m1 = newMap();
        m1.put( "one", largeString() );
        m1.put( "two", largeString() );

        String key2 = "testing2";

        Map<String, Object> m2 = newMap();
        m2.put( "three", largeString() );
        m2.put( "four", largeString() );

        node1.setProperty( key1, m1 );
        node1.setProperty( key2, m2 );

        newTransaction();
        clearCache();

        assertMapEquals( m1, (Map<String, Object>) node1.getProperty( key1 ) );
        assertMapEquals( m2, (Map<String, Object>) node1.getProperty( key2 ) );
    }

    @Test
    public void testCompoundTypes()
    {
        String key = "testing";

        long beforeDAR = dynamicArrayRecordsInUse();
        long beforeDSR = dynamicStringRecordsInUse();

        Map<String, Object> m = newMap();
        m.put( "byte", Byte.valueOf( "123" ) );
        m.put( "boolean", true );
        m.put( "char", 'x');
        m.put( "int", 0xDEADBEEF);
        m.put( "long", 1L << 33);
        m.put( "double", 3.1415);
        m.put( "string", "test" );
        m.put( "largeString", largeString() );

        m.put( "byte[]", new byte[]{1, 2, 3} );
        m.put( "boolean[]", new boolean[]{true, false, true} );
        m.put( "char[]", new char[]{'h', 'e', 'l', 'l', '0'} );
        m.put( "int[]", new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 0} );
        m.put( "long[]", new long[]{0, 9, 8, 7, 6, 5, 4, 3, 2, 1} );
        m.put( "double[]", new double[]{3.0, 0.14, 0.15} );
        m.put( "string[]", new String[]{"hello", "world"} );
        m.put( "largeString[]", new String[]{largeString(), largeString()} );

        String sameValue = "saveValue";
        m.put( "value1", sameValue );
        m.put( "value2", sameValue );

        Map<String, Object> nested = new HashMap<String, Object>(m);
        m.put( "nested1", nested );
        m.put( "nested2", nested );

        node1.setProperty( key, m );

        long afterSetDAR = dynamicArrayRecordsInUse();
        long afterSetDSR = dynamicStringRecordsInUse();

        assertTrue( afterSetDAR > beforeDAR );
        assertTrue( afterSetDSR > beforeDSR );

        newTransaction();
        clearCache();

        assertMapEquals( m, (Map<String, Object>) node1.getProperty( key ) );

        newTransaction();
        clearCache();

        node1.removeProperty( key );

        newTransaction();
        clearCache();

        assertNull( node1.getProperty( key, null ) );

        long afterRemoveDAR = dynamicArrayRecordsInUse();
        long afterRemoveDSR = dynamicStringRecordsInUse();

        assertEquals( beforeDAR, afterRemoveDAR );
        assertEquals( beforeDSR, afterRemoveDSR );
    }

    @Test( expected = IllegalArgumentException.class )
    public void testArrayOfCompounds()
    {
        Object[] array = new Object[10];

        for ( int i = 0 ; i < array.length ; i++ )
        {
            Map<String, Object> m = newMap();
            m.put( "index", i );
            m.put( "position", String.valueOf( i + 1 ) );
            array[i] = m;
        }

        node1.setProperty( "compound array", array );
        fail( "Property setting should fail due to lack of compound array support" );
    }

    @Test
    public void testCompoundKeyReuse()
    {
        String reusedKey1 = "name1";
        String reusedKey2 = "name2";
        String reusedKey3 = "name3";

        node1.setProperty( reusedKey1, "value1" );
        node1.setProperty( reusedKey2, "value2" );
        node1.setProperty( reusedKey3, "some value that is not a map");

        newTransaction();
        clearCache();

        long beforeNR = nameRecordsInUse();

        Map<String, Object> m = newMap();
        m.put( reusedKey1, "mapValue1" );
        m.put( reusedKey2, "mapValue2" );

        node1.setProperty( reusedKey1, "otherValue1" );
        node1.setProperty( reusedKey2, "otherValue2" );
        node1.setProperty( reusedKey3, m );

        newTransaction();
        clearCache();

        long afterNR = nameRecordsInUse();

        assertEquals( beforeNR, afterNR );

        assertEquals( "otherValue1", node1.getProperty( reusedKey1 ) );
        assertEquals( "otherValue2", node1.getProperty( reusedKey2 ) );
        assertMapEquals( m, (Map<String, Object>)node1.getProperty( reusedKey3 ) );
    }

    private String largeString()
    {
        StringBuilder sb = new StringBuilder( );
        for ( int i = 0 ; i < 1000 ; i++ ) {
            sb.append( i );
        }
        return sb.toString();
    }

    private Map<String, Object> newMap()
    {
        return new HashMap<String, Object>();
    }

    private void assertMapEquals( Map<String, Object> expected, Map<String, Object> m )
    {
        assertEquals( "Map sizes should be equal", expected.size(), m.size() );
        for ( String key : expected.keySet() )
        {
            if ( expected.get( key ).getClass().isArray() )
            {
                Class<?> component = expected.get( key ).getClass().getComponentType();
                if ( !component.isPrimitive() ) // then it is String, cast to
                                                // Object[] is safe
                {
                    assertArrayEquals(
                            (Object[]) expected.get( key ),
                            (Object[]) m.get( key ) );
                }
                else
                {
                    if ( component == Integer.TYPE )
                    {
                        assertArrayEquals(
                                (int[]) expected.get( key ),
                                (int[]) m.get( key ) );
                    }
                    else if ( component == Boolean.TYPE )
                    {
                        // NB: There is no assertArrayEquals(for boolean[]
                        assertTrue( Arrays.equals(
                                (boolean[]) expected.get( key ),
                                (boolean[]) m.get( key ) ) );
                    }
                    else if ( component == Byte.TYPE )
                    {
                        assertArrayEquals(
                                (byte[]) expected.get( key ),
                                (byte[]) m.get( key ) );
                    }
                    else if ( component == Character.TYPE )
                    {
                        assertArrayEquals(
                                (char[]) expected.get( key ),
                                (char[]) m.get( key ) );
                    }
                    else if ( component == Long.TYPE )
                    {
                        assertArrayEquals(
                                (long[]) expected.get( key ),
                                (long[]) m.get( key ) );
                    }
                    else if ( component == Float.TYPE )
                    {
                        assertArrayEquals(
                                (float[]) expected.get( key ),
                                (float[]) m.get( key ), 0f );
                    }
                    else if ( component == Double.TYPE )
                    {
                        assertArrayEquals(
                                (double[]) expected.get( key ),
                                (double[]) m.get( key ), 0.0 );
                    }
                    else if ( component == Short.TYPE )
                    {
                        assertArrayEquals(
                                (short[]) expected.get( key ),
                                (short[]) m.get( key ) );
                    }
                }
            }
            else if ( expected.get( key ) instanceof Map )
            {
                assertMapEquals( (Map<String, Object>)expected.get( key ) , (Map<String, Object>)m.get( key ) );
            }
            else
            {
                assertEquals( expected.get( key ), m.get( key ) );
            }
        }
    }
}

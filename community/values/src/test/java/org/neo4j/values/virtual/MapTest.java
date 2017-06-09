/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.values.virtual;

import org.junit.Test;

import org.neo4j.values.AnyValue;
import org.neo4j.values.Values;
import org.neo4j.values.VirtualValue;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MapTest
{
    @Test
    public void shouldBeEqualToItself()
    {
        assertEqual(
                map( 1, false, 20, new short[]{4} ),
                map( 1, false, 20, new short[]{4} ) );

        assertEqual(
                map( 1, 101L, 20, "yo" ),
                map( 1, 101L, 20, "yo" ) );
    }

    @Test
    public void shouldCoerce()
    {
        assertEqual(
                map( 1, 1, 20, 'a' ),
                map( 1, 1.0, 20, "a" ) );

        assertEqual(
                map( 1, new byte[]{1}, 20, new String[]{"x"} ),
                map( 1, new short[]{1}, 20, new char[]{'x'} ) );

        assertEqual(
                map( 1, new int[]{1}, 20, new double[]{2.0} ),
                map( 1, new float[]{1.0f}, 20, new float[]{2.0f} ) );
    }

    @Test
    public void shouldRecurse()
    {
        assertEqual(
                map( 1, map( 2, map( 3, "hi" ) ) ),
                map( 1, map( 2, map( 3, "hi" ) ) ) );
    }

    @Test
    public void shouldRecurseAndCoerce()
    {
        assertEqual(
                map( 1, map( 2, map( 3, "x" ) ) ),
                map( 1, map( 2, map( 3, 'x' ) ) ) );

        assertEqual(
                map( 1, map( 2, map( 3, 1.0 ) ) ),
                map( 1, map( 2, map( 3, 1 ) ) ) );
    }

    private VirtualValue map( Object... keyOrVal )
    {
        assert keyOrVal.length % 2 == 0;
        int[] keys = new int[keyOrVal.length / 2];
        AnyValue[] values = new AnyValue[keyOrVal.length / 2];
        for ( int i = 0; i < keyOrVal.length; i+=2 )
        {
            keys[i/2] = (Integer)keyOrVal[i];
            values[i/2] = toAnyValue( keyOrVal[i+1] );
        }
        return VirtualValues.map( keys, values );
    }

    private VirtualValue list( Object... objects )
    {
        AnyValue[] values = new AnyValue[objects.length];
        for ( int i = 0; i < objects.length; i++ )
        {
            values[i] = toAnyValue( objects[i] );
        }
        return VirtualValues.list( values );
    }

    private AnyValue toAnyValue( Object o )
    {
        if ( o instanceof AnyValue )
        {
            return (AnyValue)o;
        }
        else
        {
            return Values.of( o );
        }
    }

    private void assertEqual( VirtualValue a, VirtualValue b )
    {
        assertTrue( "should be equal", a.equals( b ) );
        assertTrue( "should be equal", b.equals( a ) );
        assertTrue( "should have same has", a.hashCode() == b.hashCode() );
    }

    private void assertNotEqual( VirtualValue a, VirtualValue b )
    {
        assertFalse( "should not equal", a.equals( b ) );
        assertFalse( "should not equal", b.equals( a ) );
    }
}

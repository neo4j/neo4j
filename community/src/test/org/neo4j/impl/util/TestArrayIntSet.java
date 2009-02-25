/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.util;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.neo4j.impl.util.ArrayIntSet;

public class TestArrayIntSet extends TestCase
{
    public TestArrayIntSet( String testName )
    {
        super( testName );
    }

    public static void main( java.lang.String[] args )
    {
        junit.textui.TestRunner.run( suite() );
    }

    public static Test suite()
    {
        TestSuite suite = new TestSuite( TestArrayIntSet.class );
        return suite;
    }

    public void setUp()
    {
    }

    public void tearDown()
    {
    }

    public void testArrayIntSet()
    {
        ArrayIntSet set = new ArrayIntSet();

        set.add( 1 );
        set.add( 2 );
        set.add( 3 );
        set.add( 4 );
        set.add( 5 );
        set.add( 6 );
        set.add( 7 );
        set.add( 8 );
        set.add( 9 );
        set.add( 10 );

        int count = 0;
        for ( int value : set.values() )
        {
            assertTrue( set.contains( value ) );
            count++;
        }
        assertEquals( 10, count );

        assertTrue( set.remove( 2 ) );
        assertTrue( set.remove( 9 ) );
        assertTrue( set.remove( 5 ) );
        assertTrue( !set.remove( 2 ) );
        assertTrue( !set.remove( 9 ) );
        assertTrue( !set.remove( 5 ) );

        count = 0;
        for ( int value : set.values() )
        {
            assertTrue( set.contains( value ) );
            count++;
        }
        assertEquals( 7, count );

        assertTrue( set.remove( 3 ) );
        assertTrue( set.remove( 8 ) );
        assertTrue( set.remove( 4 ) );
        assertTrue( !set.remove( 3 ) );
        assertTrue( !set.remove( 8 ) );
        assertTrue( !set.remove( 4 ) );

        count = 0;
        for ( int value : set.values() )
        {
            assertTrue( set.contains( value ) );
            count++;
        }
        assertEquals( 4, count );

        assertTrue( set.remove( 1 ) );
        assertTrue( set.remove( 7 ) );
        assertTrue( set.remove( 6 ) );
        assertTrue( !set.remove( 1 ) );
        assertTrue( !set.remove( 7 ) );
        assertTrue( !set.remove( 6 ) );

        count = 0;
        for ( int value : set.values() )
        {
            assertTrue( set.contains( value ) );
            count++;
        }
        assertEquals( 1, count );

        assertTrue( set.remove( 10 ) );
        assertTrue( !set.remove( 10 ) );

        count = 0;
        for ( int value : set.values() )
        {
            assertTrue( set.contains( value ) );
            count++;
        }
        assertEquals( 0, count );
    }
    
    public void testContains()
    {
        ArrayIntSet set = new ArrayIntSet();
        for ( int i = 0; i < 10; i++ )
        {
            set.add( i );
            assertTrue( set.contains( i ) );
        }
        for ( int i = 0; i < 10; i++ )
        {
            assertTrue( set.contains( i ) );
        }
        for ( int i = 0; i < 10; i+=2 )
        {
            set.remove( i );
            assertTrue( !set.contains( i ) );
        }
        for ( int i = 0; i < 10; i++ )
        {
            if ( i % 2 == 0 )
            {
                assertTrue( !set.contains( i ) );
            }
            else
            {
                assertTrue( set.contains( i ) );
            }
        }
        
        for ( int i = 0; i < 1000; i++ )
        {
            set.add( i );
            assertTrue( set.contains( i ) );
        }
        for ( int i = 0; i < 1000; i++ )
        {
            assertTrue( set.contains( i ) );
        }
        for ( int i = 0; i < 1000; i+=2 )
        {
            set.remove( i );
            assertTrue( !set.contains( i ) );
        }
        for ( int i = 0; i < 1000; i++ )
        {
            if ( i % 2 == 0 )
            {
                assertTrue( !set.contains( i ) );
            }
            else
            {
                assertTrue( set.contains( i ) );
            }
        }
    }
}
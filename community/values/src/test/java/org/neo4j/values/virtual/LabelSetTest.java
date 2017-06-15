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

import org.neo4j.values.TextValue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.values.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.labels;

public class LabelSetTest
{
    @Test
    public void shouldEqualItself()
    {
        VirtualValueTestUtil.assertEqual( labelSet(), labelSet() );
        VirtualValueTestUtil.assertEqual( labelSet( 1 ), labelSet( 1 ) );
        VirtualValueTestUtil.assertEqual( labelSet( 1, 2 ), labelSet( 1, 2 ) );
        VirtualValueTestUtil.assertEqual( labelSet( 4, 5, 12, 13, 100 ), labelSet( 4, 5, 12, 13, 100 ) );
    }

    @Test
    public void shouldNotEqual()
    {
        VirtualValueTestUtil.assertNotEqual( labelSet(), labelSet( 1 ) );
        VirtualValueTestUtil.assertNotEqual( labelSet( 1 ), labelSet( 2 ) );
        VirtualValueTestUtil.assertNotEqual( labelSet( 1 ), labelSet( 1, 2 ) );
        VirtualValueTestUtil.assertNotEqual( labelSet(), labelSet( 1, 2 ) );
        VirtualValueTestUtil.assertNotEqual( labelSet( 1, 2 ), labelSet( 3, 4 ) );
        VirtualValueTestUtil.assertNotEqual( labelSet( 1, 2 ), labelSet( 1, 3 ) );
    }

    @Test
    public void shouldAssertSorted()
    {
        try
        {
            labels( stringValue( "M" ), stringValue( "L" ) );
            fail( "should throw on nonsorted input" );
        }
        catch ( Throwable t )
        {
            assertEquals( t.getClass(), AssertionError.class );
        }
    }

    @Test
    public void shouldAssertSet()
    {
        try
        {
            labels( stringValue( "L" ), stringValue( "L" ) );
            fail( "should throw on nonunique input" );
        }
        catch ( Throwable t )
        {
            assertEquals( t.getClass(), AssertionError.class );
        }
    }

    private LabelSet labelSet( int... ids )
    {
        TextValue[] labelValues = new TextValue[ids.length];
        for ( int i = 0; i < ids.length; i++ )
        {
            labelValues[i] = stringValue( Integer.toString( ids[i] ) );
        }
        return labels( labelValues );
    }
}

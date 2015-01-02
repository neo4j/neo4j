/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.api.properties;

import java.util.concurrent.Callable;

import org.junit.Test;

import org.neo4j.helpers.ArrayUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LazyPropertyTest
{
    @Test
    public void shouldLoadLazyStringProperty() throws Exception
    {
        // given
        LazyStringProperty property = new LazyStringProperty( 0, value( "person" ) );
        // when / then
        assertEquals( "person", property.value() );
    }

    @Test
    public void shouldLoadLazyStringPropertyOnlyOnce() throws Exception
    {
        // given
        LazyStringProperty property = new LazyStringProperty( 0, value( "person" ) );
        // when / then
        assertEquals( "person", property.value() );
        assertEquals( "person", property.value() );
    }

    @Test
    public void shouldExhibitCorrectEqualityForBooleanArray() throws Exception
    {
        verifyCorrectValueEqualityForLazyArrayProperty( new boolean[] {} );
    }

    @Test
    public void shouldExhibitCorrectEqualityForByteArray() throws Exception
    {
        verifyCorrectValueEqualityForLazyArrayProperty( new byte[] {} );
    }

    @Test
    public void shouldExhibitCorrectEqualityForShortArray() throws Exception
    {
        verifyCorrectValueEqualityForLazyArrayProperty( new short[] {} );
    }

    @Test
    public void shouldExhibitCorrectEqualityForCharArray() throws Exception
    {
        verifyCorrectValueEqualityForLazyArrayProperty( new char[] {} );
    }

    @Test
    public void shouldExhibitCorrectEqualityForIntArray() throws Exception
    {
        verifyCorrectValueEqualityForLazyArrayProperty( new int[] {} );
    }

    @Test
    public void shouldExhibitCorrectEqualityForLongArray() throws Exception
    {
        verifyCorrectValueEqualityForLazyArrayProperty( new long[] {} );
    }

    @Test
    public void shouldExhibitCorrectEqualityForFloatArray() throws Exception
    {
        verifyCorrectValueEqualityForLazyArrayProperty( new float[] {} );
    }

    @Test
    public void shouldExhibitCorrectEqualityForDoubleArray() throws Exception
    {
        verifyCorrectValueEqualityForLazyArrayProperty( new double[] {} );
    }

    @Test
    public void shouldExhibitCorrectEqualityForStringArray() throws Exception
    {
        verifyCorrectValueEqualityForLazyArrayProperty( new String[] {} );
    }

    private static void verifyCorrectValueEqualityForLazyArrayProperty( Object array )
    {
        // given
        LazyArrayProperty property = new LazyArrayProperty( 0, value( ArrayUtil.clone( array ) ) );
        // when/then
        assertTrue( "value should be reported equal with same type", property.valueEquals( array ) );
        // when
        for ( Object value : new Object[] { new boolean[] {}, new byte[] {}, new short[] {}, new char[] {},
                new int[] {}, new long[] {}, new float[] {}, new double[] {}, new String[] {}, } )
        {
            if ( value.getClass() == array.getClass() )
            {
                continue;
            }
            // then
            assertFalse( "value should be reported inequal with different type", property.valueEquals( value ) );
        }
    }

    private static <T> Callable<T> value( final T value )
    {
        return new Callable<T>()
        {
            boolean called = false;

            @Override
            public T call() throws Exception
            {
                assertFalse( "Already called for value: " + value, called );
                called = true;
                return value;
            }
        };
    }
}

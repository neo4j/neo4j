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
package org.neo4j.internal.kernel.api;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.function.ThrowingAction;
import org.neo4j.values.storable.BufferValueWriter;
import org.neo4j.values.storable.Values;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class PropertyCursorTestBase
{
    private static final int NO_SUCH_PROPERTY = -1;

    abstract PropertyCursor emptyCursor();
    abstract PropertyCursor withValues( Map<Integer,Object> values );

    private BufferValueWriter writeBuffer = new BufferValueWriter();

    @Test
    public void shouldBeEmpty()
    {
        assertEmpty( emptyCursor() );
        assertEmpty( withValues( Collections.emptyMap() ) );
    }

    private void assertEmpty( PropertyCursor cursor )
    {
        for ( int i = 0; i < 2; i++ )
        {
            assertFalse( cursor.next() );

            assertThat( cursor.propertyKey(), equalTo( NO_SUCH_PROPERTY ) );
            assertThat( cursor.propertyValue(), equalTo( Values.NO_VALUE ) );

            assertException( cursor::booleanValue );
            assertException( cursor::stringValue );
            assertException( cursor::longValue );
            assertException( cursor::doubleValue );

            // Should we wait with specialized predicates?
//            assertFalse( cursor.valueEqualTo( 0L ) );
//            assertFalse( cursor.valueEqualTo( 0.0 ) );
//            assertFalse( cursor.valueEqualTo( "" ) );
//            assertFalse( cursor.valueMatches( Pattern.compile( ".*" ) ) );
//
//            assertFalse( cursor.valueGreaterThan( 0L ) );
//            assertFalse( cursor.valueGreaterThan( 0.0 ) );
//            assertFalse( cursor.valueLessThan( 0L ) );
//            assertFalse( cursor.valueLessThan( 0.0 ) );
//            assertFalse( cursor.valueGreaterThanOrEqualTo( 0L ) );
//            assertFalse( cursor.valueGreaterThanOrEqualTo( 0.0 ) );
//            assertFalse( cursor.valueLessThanOrEqualTo( 0L ) );
//            assertFalse( cursor.valueLessThanOrEqualTo( 0.0 ) );
        }
    }

    @Test
    public void shouldParseString()
    {
        String X = "hi";
        Map<Integer,Object> map = new HashMap<>();
        map.put( 1, X );
        PropertyCursor cursor = withValues( map );

        assertTrue( cursor.next() );
        assertThat( cursor.propertyKey(), equalTo( 1 ) );
        assertThat( cursor.stringValue(), equalTo( X ) );
        assertTrue( cursor.valueEqualTo( X ) );

        assertException( cursor::booleanValue );
        assertException( cursor::longValue );
        assertException( cursor::doubleValue );

        cursor.writeTo( writeBuffer );
        writeBuffer.assertBuffer( X );

        assertEmpty( cursor );
    }

    @Test
    public void shouldParseInteger()
    {
        long X = 2L;
        Map<Integer,Object> map = new HashMap<>();
        map.put( 1, X );
        PropertyCursor cursor = withValues( map );

        assertTrue( cursor.next() );
        assertThat( cursor.propertyKey(), equalTo( 1 ) );
        assertThat( cursor.stringValue(), equalTo( X ) );
        assertTrue( cursor.valueEqualTo( X ) );

        assertException( cursor::booleanValue );
        assertException( cursor::stringValue );
        assertException( cursor::doubleValue );

        cursor.writeTo( writeBuffer );
        writeBuffer.assertBuffer( X );

        assertEmpty( cursor );
    }

    @Test
    public void shouldParseFloat()
    {
        double X = 2.0;
        Map<Integer,Object> map = new HashMap<>();
        map.put( 1, X );
        PropertyCursor cursor = withValues( map );

        assertTrue( cursor.next() );
        assertThat( cursor.propertyKey(), equalTo( 1 ) );
        assertThat( cursor.stringValue(), equalTo( X ) );
        assertTrue( cursor.valueEqualTo( X ) );

        assertException( cursor::booleanValue );
        assertException( cursor::stringValue );
        assertException( cursor::longValue );

        cursor.writeTo( writeBuffer );
        writeBuffer.assertBuffer( X );

        assertEmpty( cursor );
    }

    @SuppressWarnings( "ConstantConditions" )
    @Test
    public void shouldParseBoolean()
    {
        boolean X = false;
        Map<Integer,Object> map = new HashMap<>();
        map.put( 1, X );
        PropertyCursor cursor = withValues( map );

        assertTrue( cursor.next() );
        assertThat( cursor.propertyKey(), equalTo( 1 ) );
        assertThat( cursor.stringValue(), equalTo( X ) );

        assertException( cursor::doubleValue );
        assertException( cursor::stringValue );
        assertException( cursor::longValue );

        cursor.writeTo( writeBuffer );
        writeBuffer.assertBuffer( X );

        assertEmpty( cursor );
    }

    private void assertException( ThrowingAction<?> action )
    {
        try
        {
            action.apply();
            fail( "Expected exception" );
        }
        catch ( Exception e )
        {
            // IGNORE
        }
    }
}

/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.driver.internal.value;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

public class FloatValueTest
{

    @Test
    public void testZeroFloatValue() throws Exception
    {
        // Given
        FloatValue value = new FloatValue( 0 );

        // Then
        assertThat( value.javaBoolean(), equalTo( false ) );
        assertThat( value.javaInteger(), equalTo( 0 ) );
        assertThat( value.javaLong(), equalTo( 0L ) );
        assertThat( value.javaFloat(), equalTo( (float) 0.0 ) );
        assertThat( value.javaDouble(), equalTo( 0.0 ) );
    }

    @Test
    public void testNonZeroFloatValue() throws Exception
    {
        // Given
        FloatValue value = new FloatValue( 6.28 );

        // Then
        assertThat( value.javaBoolean(), equalTo( true ) );
        assertThat( value.javaInteger(), equalTo( 6 ) );
        assertThat( value.javaLong(), equalTo( 6L ) );
        assertThat( value.javaFloat(), equalTo( (float) 6.28 ) );
        assertThat( value.javaDouble(), equalTo( 6.28 ) );
    }

    @Test
    public void testIsFloat() throws Exception
    {
        // Given
        FloatValue value = new FloatValue( 6.28 );

        // Then
        assertThat( value.isFloat(), equalTo( true ) );
    }

    @Test
    public void testEquals() throws Exception
    {
        // Given
        FloatValue firstValue = new FloatValue( 6.28 );
        FloatValue secondValue = new FloatValue( 6.28 );

        // Then
        assertThat( firstValue, equalTo( secondValue ) );
    }

    @Test
    public void testHashCode() throws Exception
    {
        // Given
        FloatValue value = new FloatValue( 6.28 );

        // Then
        assertThat( value.hashCode(), notNullValue() );
    }
}
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
package org.neo4j.values;

import org.junit.Test;

import java.util.concurrent.Callable;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class LazyValueTest
{
    @Test
    public void shouldLoadLazyStringProperty() throws Exception
    {
        // given
        LazyString value = new LazyString( value( "person" ) );
        // when / then
        assertThat( value.getOrLoad(), equalTo( "person" ) );
    }

    @Test
    public void shouldLoadLazyStringValueOnlyOnce() throws Exception
    {
        // given
        LazyString value = new LazyString( value( "person" ) );
        // when / then
        assertThat( value.getOrLoad(), equalTo( "person" ) );
        assertThat( value.getOrLoad(), equalTo( "person" ) );
    }

    @Test
    public void shouldLoadLazyArrayProperty() throws Exception
    {
        // given
        LazyIntArray value = new LazyIntArray( value( new int[]{1, 2, 3} ) );
        // when / then
        assertThat( value.getOrLoad(), equalTo( new int[]{1, 2, 3} ) );
    }

    @Test
    public void shouldLoadLazyArrayValueOnlyOnce() throws Exception
    {
        // given
        LazyIntArray value = new LazyIntArray( value( new int[]{1, 2, 3} ) );
        // when / then
        assertThat( value.getOrLoad(), equalTo( new int[]{1, 2, 3} ) );
        assertThat( value.getOrLoad(), equalTo( new int[]{1, 2, 3} ) );
    }

    private static <T> Callable<T> value( final T value )
    {
        return new Callable<T>()
        {
            boolean called;

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

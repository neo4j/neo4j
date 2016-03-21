/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.helpers;

import java.util.function.BiFunction;
import java.util.function.Function;

import org.junit.Test;

import org.neo4j.function.Functions;
import org.neo4j.kernel.configuration.Settings;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class FunctionsTest
{
    @Test
    public void testWithDefaults() throws Exception
    {
        assertThat( Functions.withDefaults( stringMap( "foo", "bar" )::get, from -> null )
                .apply( "foo" ), equalTo( "bar" ) );
        assertThat( Functions.withDefaults( stringMap( "foo", "bar" )::get, stringMap( "foo", "xyzzy" )::get )
                .apply( "foo" ), equalTo( "xyzzy" ) );
    }

    @Test
    public void testConstant() throws Exception
    {
        assertThat( Functions.constant( "foo" ).apply( "bar" ), equalTo( "foo" ) );

    }

    @Test
    public void testCompose2() throws Exception
    {
        BiFunction<Integer, Integer, Integer> add = ( from1, from2 ) -> from1 + from2;

        BiFunction<Integer, Integer, Integer> mult = ( from1, from2 ) -> from1 * from2;

        assertThat( Functions.<Integer, Integer>compose2().apply( add, mult ).apply( 2, 3 ), equalTo( 9 ));
    }

    @Test
    public void testCompose() throws Exception
    {
        Function<Integer, Integer> inc = value -> value + 1;
        assertThat( Functions.<String, Integer, Integer>compose().apply( Settings.INTEGER, inc ).apply( "3" ), equalTo( 4 ));
    }
}

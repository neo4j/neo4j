/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.codegen;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.test.rule.EnterpriseDatabaseRule;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.helpers.collection.MapUtil.map;

public class CompiledRuntimeEchoIT
{
    @Rule
    public final EnterpriseDatabaseRule db = new EnterpriseDatabaseRule();

    @Test
    public void shouldBeAbleToEchoMaps()
    {
        echo( map( "foo", "bar" ) );
        echo( map( "foo", 42 ) );
        echo( map( "foo", map( "bar", map( "baz", 1337 ) ) ) );
    }

    @Test
    public void shouldBeAbleToEchoLists()
    {
        echo( asList( 1, 2, 3 ) );
        echo( asList( "a", 1, 17 ) );
        echo( map( "foo", asList( asList( 1, 2, 3 ), "foo" ) ) );
    }

    @Test
    public void shouldBeAbleToEchoListsOfMaps()
    {
        echo( singletonList( map( "foo", "bar" ) ) );
        echo( asList( "a", 1, 17, map( "foo", asList( 1, 2, 3 ) ) ) );
        echo( asList( "foo", asList( map( "bar", 42 ), "foo" ) ) );
    }

    @Test
    public void shouldBeAbleToEchoMapsOfLists()
    {
        echo( map( "foo", singletonList( "bar" ) ) );
        echo( map( "foo", singletonList( map( "bar", map( "baz", 1337 ) ) ) ) );
    }

    private void echo( Object value )
    {
        Object result = db.execute( "CYPHER runtime=compiled RETURN {p} AS p", map( "p", value ) ).next().get( "p" );
        assertThat( result, equalTo( value ) );
    }
}

/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.server.scripting.javascript;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;

public class TestJavascriptExecutor
{

    @Test
    public void shouldExecuteBasicScript()
    {
        // Given
        JavascriptExecutor executor = new JavascriptExecutor( "1337;" );

        // When
        Object out = executor.execute( null );

        // Then
        assertThat( out, not(nullValue()));
        assertThat( out, is( 1337 ) );
    }

    @Test
    public void shouldAllowContextVariables()
    {
        // Given
        JavascriptExecutor executor = new JavascriptExecutor( "myVar;" );

        Map<String, Object> ctx = new HashMap<>();
        ctx.put( "myVar", 1338 );

        // When
        Object out = executor.execute( ctx );

        // Then
        assertThat( out, not( nullValue() ));
        assertThat( out, is( 1338 ));
    }

    @Test
    public void shouldBeAbleToReuseExecutor()
    {
        // Given
        JavascriptExecutor executor = new JavascriptExecutor( "1337;" );

        // When
        Object out1 = executor.execute( null );
        Object out2 = executor.execute( null );

        // Then
        assertThat( out1, is( 1337 ) );
        assertThat( out2, is( 1337 ) );
    }

    @Test
    public void varsSetInOneExecutionShouldNotBeAvailableInAnother()
    {
        // Given
        JavascriptExecutor executor = new JavascriptExecutor(
                "if(firstRun) { " +
                "  this['theVar'] = 'boo'; " +
                "} else { " +
                "  this['theVar']; " +
                "}" );

        Map<String, Object> ctx = new HashMap<>();

        // When
        ctx.put( "firstRun", true );
        Object out1 = executor.execute( ctx );

        ctx.put( "firstRun", false );
        Object out2 = executor.execute( ctx );

        // Then
        assertThat( out1, is( "boo" ) );
        assertThat( out2, is( nullValue()) );
    }

}

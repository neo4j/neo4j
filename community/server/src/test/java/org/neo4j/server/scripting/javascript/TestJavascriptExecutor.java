/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.scripting.javascript;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class TestJavascriptExecutor
{

    @Test
    public void shouldExecuteBasicScript() throws Exception
    {
        // Given
        JavascriptExecutor executor = new JavascriptExecutor( "1337;" );

        // When
        Object out = executor.execute( null );

        // Then
        assertThat( out, not(nullValue()));
        assertThat( (Integer) out, is( 1337 ) );
    }

    @Test
    public void shouldAllowContextVariables() throws Exception
    {
        // Given
        JavascriptExecutor executor = new JavascriptExecutor( "myVar;" );

        Map<String, Object> ctx = new HashMap<String, Object>();
        ctx.put( "myVar", 1338 );

        // When
        Object out = executor.execute( ctx );

        // Then
        assertThat( out, not( nullValue() ));
        assertThat( (Integer) out, is( 1338 ));
    }

    @Test
    public void shouldBeAbleToReuseExecutor() throws Exception
    {
        // Given
        JavascriptExecutor executor = new JavascriptExecutor( "1337;" );

        // When
        Object out1 = executor.execute( null );
        Object out2 = executor.execute( null );

        // Then
        assertThat( (Integer) out1, is( 1337 ) );
        assertThat( (Integer) out2, is( 1337 ) );
    }

    @Test
    public void varsSetInOneExecutionShouldNotBeAvailableInAnother() throws Exception
    {
        // Given
        JavascriptExecutor executor = new JavascriptExecutor(
                "if(firstRun) { " +
                "  this['theVar'] = 'boo'; " +
                "} else { " +
                "  this['theVar']; " +
                "}" );


        Map<String, Object> ctx = new HashMap<String, Object>();

        // When
        ctx.put( "firstRun", true );
        Object out1 = executor.execute( ctx );

        ctx.put( "firstRun", false );
        Object out2 = executor.execute( ctx );

        // Then
        assertThat( (String) out1, is( "boo" ) );
        assertThat( out2, is( nullValue()) );
    }

}

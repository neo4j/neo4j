/*
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
package org.neo4j.kernel.procedure.impl;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedures.ProcedureSource;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.kernel.api.procedures.ProcedureSignature.procedureSignature;

public class StandardProcedureCompilerTest
{
    @Rule
    public ExpectedException expected = ExpectedException.none();

    public final StandardProcedureCompiler compiler = new StandardProcedureCompiler();

    @Test
    public void shouldFailIfLanguageHandlerDoesNotExist() throws Throwable
    {
        // Expect
        expected.expect( ProcedureException.class );
        expected.expectMessage( "The procedure `snigel.stretch` cannot be compiled, no language handler for `snigescript` registered." );

        // When
        compiler.compile( new ProcedureSource( procedureSignature( "snigel.stretch" ).build(), "snigescript", ".." ) );
    }

    @Test
    public void shouldCompileAndCallProcedure() throws Throwable
    {
        // Given
        compiler.addLanguageHandler( "snigescript", new SnigelScriptHandler() );

        CollectingProcVisitor visitor = new CollectingProcVisitor();
        List<Object> arguments = asList( (Object)1, 2, 3 );

        // When
        compiler.compile(new ProcedureSource( procedureSignature( "snigel.stretch" ).build(), "snigescript", ".." )).call( arguments, visitor );

        // Then
        assertThat( visitor.records, equalTo( asList( arguments ) ));
    }
}
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
package org.neo4j.kernel.procedure.impl.js;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedures.ProcedureSource;
import org.neo4j.kernel.procedure.CompiledProcedure;
import org.neo4j.kernel.procedure.impl.CollectingProcVisitor;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.kernel.api.Neo4jTypes.NTText;
import static org.neo4j.kernel.api.procedures.ProcedureSignature.procedureSignature;

public class JavascriptLanguageHandlerTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldCompileAndRunBasicProcedure() throws Throwable
    {
        // Given
        JavascriptLanguageHandler handler = new JavascriptLanguageHandler();

        CompiledProcedure proc = handler.compile( new ProcedureSource(
                procedureSignature( "steve.become_mayor" )
                        .in( "input", NTText )
                        .out( "result", NTText ).build(),
                "javascript", "emit( 'Became ' + input )" ) );

        // When
        CollectingProcVisitor results = new CollectingProcVisitor();
        proc.call( asList((Object)"Mayor"), results );

        // Then
        assertThat( results.records, equalTo( asList(
                asList( (Object) "Became Mayor" ) ) ));
    }

    @Test
    public void shouldGiveHelpfulCompileError() throws Throwable
    {
        // Given
        JavascriptLanguageHandler handler = new JavascriptLanguageHandler();

        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( "Failed to compile javascript procedure" );
        exception.expectMessage( "this makes no sense" );

        // When
        handler.compile( new ProcedureSource(
                procedureSignature( "steve.become_mayor" ).build(),
                "javascript", "this makes no sense" ) );
    }
}
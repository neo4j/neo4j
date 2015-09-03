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
package org.neo4j.kernel.impl.api.store;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedures.ProcedureSource;
import org.neo4j.kernel.procedure.impl.SnigelScriptHandler;
import org.neo4j.kernel.procedure.impl.StandardProcedureCompiler;

import static org.neo4j.kernel.api.procedures.ProcedureSignature.procedureSignature;

public class ProcedureCacheTest
{
    @Rule
    public ExpectedException expected = ExpectedException.none();

    private final ProcedureSource source = new ProcedureSource( procedureSignature( "snigel.stretch" ).build(), "snigescript", ".." );

    public final StandardProcedureCompiler engine = new StandardProcedureCompiler();
    public final ProcedureCache cache = new ProcedureCache( engine );

    @Test
    public void shouldNotAllowCompilingNonExistantProcedure() throws Throwable
    {
        // Expect
        expected.expect( ProcedureException.class );
        expected.expectMessage( "No such procedure" );

        // When
        cache.getCompiled( procedureSignature( "jakesStuff.proc" ).build().name() );
    }

    @Test
    public void shouldNotAllowCompilingDroppedProcedure() throws Throwable
    {
        // Given
        engine.addLanguageHandler( "snigescript", new SnigelScriptHandler() );
        cache.createProcedure( source );
        cache.dropProcedure( source );

        // Expect
        expected.expect( ProcedureException.class );

        // When
        cache.getCompiled( source.signature().name() );
    }

    @Test
    public void shouldNotGiveAccessToCompiledProcedureAfterItIsDropped() throws Throwable
    {
        // Given
        engine.addLanguageHandler( "snigescript", new SnigelScriptHandler() );
        cache.createProcedure( source );

        // And given we have compiled it
        cache.getCompiled( source.signature().name() );

        // and then dropped it
        cache.dropProcedure( source );

        // Expect
        expected.expect( ProcedureException.class );

        // When we get the compiled thing
        cache.getCompiled( source.signature().name() );
    }

}
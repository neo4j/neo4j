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
package org.neo4j.kernel.impl.proc;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Optional;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.proc.BasicContext;
import org.neo4j.kernel.api.proc.CallableUserFunction;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.api.proc.Key;
import org.neo4j.kernel.api.proc.Neo4jTypes;
import org.neo4j.kernel.api.proc.UserFunctionSignature;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.neo4j.kernel.api.proc.Key.key;
import static org.neo4j.kernel.api.proc.UserFunctionSignature.functionSignature;

public class UserFunctionsTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private final Procedures procs = new Procedures();
    private final UserFunctionSignature signature =
            functionSignature( "org", "myproc" )
                    .out( Neo4jTypes.NTAny)
                    .build();
    private final CallableUserFunction function = function( signature );

    @Test
    public void shouldGetRegisteredFunction() throws Throwable
    {
        // When
        procs.register( function );

        // Then
        assertThat( procs.function( signature.name() ).get(), equalTo( signature ) );
    }

    @Test
    public void shouldGetAllRegisteredFunctions() throws Throwable
    {
        // When
        procs.register( function( functionSignature( "org", "myproc1" ).out(Neo4jTypes.NTAny).build() ) );
        procs.register( function( functionSignature( "org", "myproc2" ).out(Neo4jTypes.NTAny).build() ) );
        procs.register( function( functionSignature( "org", "myproc3" ).out(Neo4jTypes.NTAny).build() ) );

        // Then
        List<UserFunctionSignature> signatures = Iterables.asList( procs.getAllFunctions() );
        assertThat( signatures, containsInAnyOrder(
                functionSignature( "org", "myproc1" ).out(Neo4jTypes.NTAny).build(),
                functionSignature( "org", "myproc2" ).out(Neo4jTypes.NTAny).build(),
                functionSignature( "org", "myproc3" ).out(Neo4jTypes.NTAny).build() ) );
    }

    @Test
    public void shouldCallRegisteredFunction() throws Throwable
    {
        // Given
        procs.register( function );

        // When
        Object result = procs.callFunction( new BasicContext(), signature.name(), new Object[]{1337} );

        // Then
        assertThat( result , equalTo( new Object[]{1337} ) );
    }

    @Test
    public void shouldNotAllowCallingNonExistingFunction() throws Throwable
    {
        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( "There is no function with the name `org.myproc` registered for this " +
                                 "database instance. Please ensure you've spelled the " +
                                 "function name correctly and that the function is properly deployed." );

        // When
        procs.callFunction( new BasicContext(), signature.name(), new Object[]{1337} );
    }

    @Test
    public void shouldNotAllowRegisteringConflictingName() throws Throwable
    {
        // Given
        procs.register( function );

        // Expect
        exception.expect( ProcedureException.class );
        exception.expectMessage( "Unable to register function, because the name `org.myproc` is already in use." );

        // When
        procs.register( function );
    }

    @Test
    public void shouldSignalNonExistingFunction() throws Throwable
    {
        // When
        assertThat(procs.function( signature.name() ), is( Optional.empty()));
    }

    @Test
    public void shouldMakeContextAvailable() throws Throwable
    {
        // Given
        Key<String> someKey = key("someKey", String.class);

        procs.register( new CallableUserFunction.BasicUserFunction( signature )
        {
            @Override
            public Object apply( Context ctx, Object[] input ) throws ProcedureException
            {
                return ctx.get( someKey );
            }
        } );

        BasicContext ctx = new BasicContext();
        ctx.put( someKey, "hello, world" );

        // When
        Object result = procs.callFunction( ctx, signature.name(), new Object[0] );

        // Then
        assertThat( result, equalTo("hello, world" ) );
    }

    private CallableUserFunction function( UserFunctionSignature signature )
    {
        return new CallableUserFunction.BasicUserFunction( signature )
        {
            @Override
            public Object apply( Context ctx, Object[] input )
            {
                return input;
            }
        };
    }
}

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
package org.neo4j.procedure.impl;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.UserAggregator;
import org.neo4j.internal.kernel.api.procs.UserFunctionHandle;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.kernel.api.procedure.CallableUserAggregationFunction;
import org.neo4j.kernel.api.procedure.CallableUserFunction;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.values.AnyValue;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.storable.Values;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.neo4j.internal.kernel.api.procs.UserFunctionSignature.functionSignature;
import static org.neo4j.kernel.api.procedure.BasicContext.buildContext;
import static org.neo4j.values.storable.Values.numberValue;

class UserFunctionsTest
{
    private final GlobalProceduresRegistry procs = new GlobalProceduresRegistry();
    private final UserFunctionSignature signature =
            functionSignature( "org", "myproc" )
                    .out( Neo4jTypes.NTAny)
                    .build();
    private final CallableUserFunction function = function( signature );
    private final DependencyResolver dependencyResolver = new Dependencies();
    private final ValueMapper<Object> valueMapper = new DefaultValueMapper( mock( EmbeddedProxySPI.class ) );

    @Test
    void shouldGetRegisteredFunction() throws Throwable
    {
        // When
        procs.register( function );

        // Then
        assertThat( procs.function( signature.name() ).signature(), equalTo( signature ) );
    }

    @Test
    void shouldGetAllRegisteredFunctions() throws Throwable
    {
        // When
        procs.register( function( functionSignature( "org", "myproc1" ).out(Neo4jTypes.NTAny).build() ) );
        procs.register( function( functionSignature( "org", "myproc2" ).out(Neo4jTypes.NTAny).build() ) );
        procs.register( function( functionSignature( "org", "myproc3" ).out(Neo4jTypes.NTAny).build() ) );

        // Then
        List<UserFunctionSignature> signatures = Iterables.asList( procs.getAllNonAggregatingFunctions().collect( Collectors.toSet()) );
        assertThat( signatures, containsInAnyOrder(
                functionSignature( "org", "myproc1" ).out(Neo4jTypes.NTAny).build(),
                functionSignature( "org", "myproc2" ).out(Neo4jTypes.NTAny).build(),
                functionSignature( "org", "myproc3" ).out(Neo4jTypes.NTAny).build() ) );

        // And
        signatures = Iterables.asList( procs.getAllAggregatingFunctions().collect( Collectors.toSet()) );
        assertThat( signatures, empty() );
    }

    @Test
    void shouldGetRegisteredAggregationFunctions() throws Throwable
    {
        // When
        procs.register( function( functionSignature( "org", "myfunc1" ).out(Neo4jTypes.NTAny).build() ) );
        procs.register( function( functionSignature( "org", "myfunc2" ).out(Neo4jTypes.NTAny).build() ) );
        procs.register( aggregationFunction( functionSignature( "org", "myaggrfunc1" ).out(Neo4jTypes.NTAny).build() ) );

        // Then
        List<UserFunctionSignature> signatures = Iterables.asList( procs.getAllNonAggregatingFunctions().collect( Collectors.toSet()) );
        assertThat( signatures, containsInAnyOrder(
                functionSignature( "org", "myfunc1" ).out(Neo4jTypes.NTAny).build(),
                functionSignature( "org", "myfunc2" ).out(Neo4jTypes.NTAny).build() ) );

        // And
        signatures = Iterables.asList( procs.getAllAggregatingFunctions().collect( Collectors.toSet()) );
        assertThat( signatures, containsInAnyOrder(
                functionSignature( "org", "myaggrfunc1" ).out(Neo4jTypes.NTAny).build() ) );
    }

    @Test
    void shouldCallRegisteredFunction() throws Throwable
    {
        // Given
        procs.register( function );
        int functionId = procs.function( signature.name() ).id();

        // When
        Object result = procs.callFunction( prepareContext(), functionId, new AnyValue[] {numberValue( 1337 )} );

        // Then
        assertThat( result , equalTo( Values.of(1337) ) );
    }

    @Test
    void shouldNotAllowCallingNonExistingFunction()
    {
        UserFunctionHandle functionHandle = procs.function( signature.name() );
        ProcedureException exception = assertThrows( ProcedureException.class,
                () -> procs.callFunction( prepareContext(), functionHandle != null ? functionHandle.id() : -1, new AnyValue[]{numberValue( 1337 )} ) );
        assertThat( exception.getMessage(), equalTo( "There is no function with the internal id `-1` registered for this database instance." ) );
    }

    @Test
    void shouldNotAllowRegisteringConflictingName() throws Throwable
    {
        // Given
        procs.register( function );

        ProcedureException exception = assertThrows( ProcedureException.class, () -> procs.register( function ) );
        assertThat( exception.getMessage(), equalTo( "Unable to register function, because the name `org.myproc` is already in use." ) );
    }

    @Test
    void shouldSignalNonExistingFunction()
    {
        // When
        assertThat(procs.function( signature.name() ), nullValue());
    }

    @Test
    void shouldMakeContextAvailable() throws Throwable
    {
        // Given
        procs.register( new CallableUserFunction.BasicUserFunction( signature )
        {
            @Override
            public AnyValue apply( Context ctx, AnyValue[] input ) throws ProcedureException
            {
                return Values.stringValue( ctx.thread().getName() );
            }
        } );

        Context ctx = prepareContext();
        int functionId = procs.function( signature.name() ).id();

        // When
        Object result = procs.callFunction( ctx, functionId, new AnyValue[0] );

        // Then
        assertThat( result, equalTo( Values.stringValue( Thread.currentThread().getName()) ) );
    }

    private CallableUserFunction function( UserFunctionSignature signature )
    {
        return new CallableUserFunction.BasicUserFunction( signature )
        {
            @Override
            public AnyValue apply( Context ctx, AnyValue[] input )
            {
                return input[0];
            }
        };
    }

    private CallableUserAggregationFunction aggregationFunction( UserFunctionSignature signature )
    {
        return new CallableUserAggregationFunction.BasicUserAggregationFunction( signature )
        {
            @Override
            public UserAggregator create( Context ctx )
            {
                return null;
            }
        };
    }

    private Context prepareContext()
    {
        return buildContext( dependencyResolver, valueMapper ).context();
    }
}

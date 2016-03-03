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
package org.neo4j.kernel.impl.api.integrationtest;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

import org.neo4j.collection.RawIterator;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Neo4jTypes;
import org.neo4j.kernel.api.proc.ProcedureSignature;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertNotNull;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTString;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureName;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureSignature;

public class ProceduresKernelIT extends KernelIntegrationTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private final ProcedureSignature signature = procedureSignature( "example", "exampleProc" )
            .in( "name", NTString )
            .out( "name", NTString ).build();

    private final CallableProcedure procedure = procedure( signature );

    @Test
    public void shouldGetProcedureByName() throws Throwable
    {
        // Given
        kernel.registerProcedure( procedure );

        // When
        ProcedureSignature found = readOperationsInNewTransaction()
                .procedureGet( new ProcedureSignature.ProcedureName( new String[]{"example"}, "exampleProc" ) );

        // Then
        assertThat( found, equalTo( signature ) );
    }

    @Test
    public void shouldGetBuiltInProcedureByName() throws Throwable
    {
        // When
        ProcedureSignature found = readOperationsInNewTransaction()
                .procedureGet( procedureName( "db", "labels" ) );

        // Then
        assertThat( found, equalTo( procedureSignature( procedureName( "db", "labels" ) )
                .out(  "label", Neo4jTypes.NTString ).build() ) );
    }

    @Test
    public void shouldGetAllProcedures() throws Throwable
    {
        // Given
        kernel.registerProcedure( procedure );
        kernel.registerProcedure( procedure( procedureSignature( "example", "exampleProc2" ).build() ) );
        kernel.registerProcedure( procedure( procedureSignature( "example", "exampleProc3" ).build() ) );

        // When
        List<ProcedureSignature> signatures =
                Iterables.asList( readOperationsInNewTransaction().proceduresGetAll() );

        // Then
        assertThat( signatures, hasItems(
            procedure.signature(),
            procedureSignature( "example", "exampleProc2" ).build(),
            procedureSignature( "example", "exampleProc3" ).build() ) );
    }

    @Test
    public void shouldCallReadOnlyProcedure() throws Throwable
    {
        // Given
        kernel.registerProcedure( procedure );

        // When
        RawIterator<Object[], ProcedureException> found = readOperationsInNewTransaction()
                .procedureCallRead( new ProcedureSignature.ProcedureName( new String[]{"example"}, "exampleProc" ), new Object[]{ 1337 } );

        // Then
        assertThat( asList( found ), contains( equalTo( new Object[]{1337} ) ) );
    }

    @Test
    public void registeredProcedureShouldGetReadOperations() throws Throwable
    {
        // Given
        kernel.registerProcedure( new CallableProcedure.BasicProcedure( signature )
        {
            @Override
            public RawIterator<Object[], ProcedureException> apply( Context ctx, Object[] input ) throws ProcedureException
            {
                return RawIterator.<Object[], ProcedureException>of( new Object[]{ ctx.get( Context.KERNEL_TRANSACTION ).acquireStatement().readOperations() } );
            }
        } );

        // When
        RawIterator<Object[], ProcedureException> stream = readOperationsInNewTransaction().procedureCallRead( signature.name(), new Object[]{""} );

        // Then
        assertNotNull( asList( stream  ).get( 0 )[0] );
    }

    private static CallableProcedure procedure( final ProcedureSignature signature )
    {
        return new CallableProcedure.BasicProcedure( signature )
        {
            @Override
            public RawIterator<Object[], ProcedureException> apply( Context ctx, Object[] input )
            {
                return RawIterator.<Object[], ProcedureException>of( input );
            }
        };
    }
}

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
package org.neo4j.kernel.impl.api.integrationtest;

import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.exceptions.schema.ProcedureConstraintViolation;
import org.neo4j.kernel.api.procedures.ProcedureSignature;

import static java.util.Arrays.asList;
import static junit.framework.TestCase.assertNull;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.kernel.api.Neo4jTypes.NTText;
import static org.neo4j.kernel.api.procedures.ProcedureSignature.procedureSignature;

public class ProceduresKernelIT extends KernelIntegrationTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private final ProcedureSignature signature = procedureSignature( "example", "exampleProc" )
            .in( "name", NTText )
            .out( "name", NTText ).build();

    @Test
    public void shouldCreateProcedure() throws Throwable
    {
        // Given
        SchemaWriteOperations ops = schemaWriteOperationsInNewTransaction();

        // When
        ops.procedureCreate( signature, "javascript", "emit(1);" );

        // Then
        Iterator<ProcedureSignature> all = ops.proceduresGetAll();
        assertThat( asCollection( all ),
                Matchers.<Collection<ProcedureSignature>>equalTo( asList( signature ) ) );

        // And when
        commit();

        // Then
        assertThat( asCollection( readOperationsInNewTransaction().proceduresGetAll() ),
                Matchers.<Collection<ProcedureSignature>>equalTo( asList( signature ) ) );
    }

    @Test
    public void shouldDropProcedure() throws Throwable
    {
        // Given
        SchemaWriteOperations ops = schemaWriteOperationsInNewTransaction();
        ops.procedureCreate( signature, "javascript", "emit(1);" );
        commit();

        // When
        ops = schemaWriteOperationsInNewTransaction();
        ops.procedureDrop( signature.name() );

        // Then
        Iterator<ProcedureSignature> all = ops.proceduresGetAll();
        assertThat( asCollection( all ),
                Matchers.<Collection<ProcedureSignature>>equalTo( Collections.<ProcedureSignature>emptyList() ) );

        // And when
        commit();

        // Then
        assertThat( asCollection( readOperationsInNewTransaction().proceduresGetAll() ),
                Matchers.<Collection<ProcedureSignature>>equalTo( Collections.<ProcedureSignature>emptyList() ) );
    }

    @Test
    public void shouldDropProcedureInSameTransaction() throws Throwable
    {
        // Given
        SchemaWriteOperations ops = schemaWriteOperationsInNewTransaction();
        ops.procedureCreate( signature, "javascript", "emit(1);" );

        // When
        ops.procedureDrop( signature.name() );

        // Then
        Iterator<ProcedureSignature> all = ops.proceduresGetAll();
        assertThat( asCollection( all ),
                Matchers.<Collection<ProcedureSignature>>equalTo( Collections.<ProcedureSignature>emptyList() ) );

        // And when
        commit();

        // Then
        assertThat( asCollection( readOperationsInNewTransaction().proceduresGetAll() ),
                Matchers.<Collection<ProcedureSignature>>equalTo( Collections.<ProcedureSignature>emptyList() ) );
    }

    @Test
    public void shouldGetProcedureByName() throws Throwable
    {
        // Given
        shouldCreateProcedure();

        // When
        ProcedureSignature found = readOperationsInNewTransaction()
                .procedureGet( new ProcedureSignature.ProcedureName( new String[]{"example"}, "exampleProc" ) )
                .signature();

        // Then
        assertThat( found, equalTo( found ) );
    }

    @Test
    public void nonexistentProcedureShouldReturnNull() throws Throwable
    {
        // When & Then
        assertNull( readOperationsInNewTransaction()
                .procedureGet( new ProcedureSignature.ProcedureName( new String[]{"example"}, "exampleProc" ) ) );
    }

    @Test
    public void droppingNonExistentProcedureShouldThrow() throws Throwable
    {
        // Expect
        exception.expect( ProcedureConstraintViolation.class );

        // When
        schemaWriteOperationsInNewTransaction().procedureDrop( new ProcedureSignature.ProcedureName( new String[]{"example"}, "exampleProc" ) );
    }

}

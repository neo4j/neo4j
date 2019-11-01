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
package org.neo4j.procedure.builtin;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.index.IndexingService;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.schema.SchemaDescriptor.forLabel;
import static org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode.backgroundRebuildAll;

class ResampleIndexProcedureTest
{
    private IndexingService indexingService;
    private IndexProcedures procedure;
    private SchemaRead schemaRead;

    @BeforeEach
    void setup()
    {
        KernelTransaction transaction = mock( KernelTransaction.class );
        schemaRead = mock( SchemaRead.class );
        when( transaction.schemaRead() ).thenReturn( schemaRead );
        indexingService = mock( IndexingService.class );
        procedure = new IndexProcedures( transaction, indexingService );
    }

    @Test
    void shouldLookUpTheIndexByName() throws ProcedureException
    {
        IndexDescriptor index = IndexPrototype.forSchema( forLabel( 0, 0 ) ).withName( "index_42" ).materialise( 42 );
        when( schemaRead.indexGetForName( anyString() ) ).thenReturn( index );

        procedure.resampleIndex( "index_42" );

        verify( schemaRead ).indexGetForName( "index_42" );
        verifyNoMoreInteractions( schemaRead );
    }

    @Test
    void shouldLookUpTheCompositeIndexByName() throws ProcedureException
    {
        IndexDescriptor index = IndexPrototype.forSchema( forLabel( 0, 0, 1 ) ).withName( "index_42" ).materialise( 42 );
        when( schemaRead.indexGetForName( anyString() ) ).thenReturn( index );

        procedure.resampleIndex( "index_42" );

        verify( schemaRead ).indexGetForName( "index_42" );
        verifyNoMoreInteractions( schemaRead );
    }

    @Test
    void shouldThrowAnExceptionIfTheIndexDoesNotExist()
    {
        when( schemaRead.indexGetForName( anyString() ) ).thenReturn( IndexDescriptor.NO_INDEX );

        ProcedureException exception = assertThrows( ProcedureException.class, () -> procedure.resampleIndex( "index_42" ) );
        assertThat( exception.status(), is( Status.Schema.IndexNotFound ) );
    }

    @Test
    void shouldTriggerResampling() throws ProcedureException
    {
        IndexDescriptor index = IndexPrototype.forSchema( forLabel( 123, 456 ) ).withName( "index_42" ).materialise( 42 );
        when( schemaRead.indexGetForName( anyString() ) ).thenReturn( index );

        procedure.resampleIndex( "index_42" );

        verify( indexingService ).triggerIndexSampling( index, backgroundRebuildAll() );
    }
}

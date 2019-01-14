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
package org.neo4j.kernel.builtinprocs;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.internal.kernel.api.CapableIndexReference;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.api.store.DefaultCapableIndexReference.fromDescriptor;

public class ResampleIndexProcedureTest
{
    private IndexingService indexingService;
    private IndexProcedures procedure;
    private TokenRead tokenRead;
    private SchemaRead schemaRead;

    @Before
    public void setup()
    {

        KernelTransaction transaction = mock( KernelTransaction.class );
        tokenRead = mock( TokenRead.class );
        schemaRead = mock( SchemaRead.class );
        procedure = new IndexProcedures( transaction, null );

        when( transaction.tokenRead() ).thenReturn( tokenRead );
        when( transaction.schemaRead() ).thenReturn( schemaRead );
        indexingService = mock( IndexingService.class );
        procedure =
                new IndexProcedures( transaction, indexingService );
    }

    @Test
    public void shouldThrowAnExceptionIfTheLabelDoesntExist()
    {
        when( tokenRead.nodeLabel( "NonExistentLabel" ) ).thenReturn( -1 );

        try
        {
            procedure.resampleIndex( ":NonExistentLabel(prop)" );
            fail( "Expected an exception" );
        }
        catch ( ProcedureException e )
        {
            assertThat( e.status(), is( Status.Schema.LabelAccessFailed ) );
        }
    }

    @Test
    public void shouldThrowAnExceptionIfThePropertyKeyDoesntExist()
    {
        when( tokenRead.propertyKey( "nonExistentProperty" ) ).thenReturn( -1 );

        try
        {
            procedure.resampleIndex( ":Label(nonExistentProperty)" );
            fail( "Expected an exception" );
        }
        catch ( ProcedureException e )
        {
            assertThat( e.status(), is( Status.Schema.PropertyKeyAccessFailed ) );
        }
    }

    @Test
    public void shouldLookUpTheIndexByLabelIdAndPropertyKeyId()
            throws ProcedureException, SchemaRuleNotFoundException
    {
        SchemaIndexDescriptor index = SchemaIndexDescriptorFactory.forLabel( 0, 0 );
        when( tokenRead.nodeLabel( anyString() ) ).thenReturn( 123 );
        when( tokenRead.propertyKey( anyString() ) ).thenReturn( 456 );
        when( schemaRead.index( anyInt(), any() ) ).thenReturn( fromDescriptor( index ) );

        procedure.resampleIndex( ":Person(name)" );

        verify( schemaRead ).index( 123, 456 );
    }

    @Test
    public void shouldLookUpTheCompositeIndexByLabelIdAndPropertyKeyId()
            throws ProcedureException, SchemaRuleNotFoundException
    {
        SchemaIndexDescriptor index = SchemaIndexDescriptorFactory.forLabel( 0, 0, 1 );
        when( tokenRead.nodeLabel( anyString() ) ).thenReturn( 123 );
        when( tokenRead.propertyKey( "name" ) ).thenReturn( 0 );
        when( tokenRead.propertyKey( "lastName" ) ).thenReturn( 1 );
        when( schemaRead.index( 123, 0, 1  ) ).thenReturn( fromDescriptor( index ) );

        procedure.resampleIndex( ":Person(name, lastName)" );

        verify( schemaRead ).index( 123, 0, 1 );
    }

    @Test
    public void shouldThrowAnExceptionIfTheIndexDoesNotExist()
            throws SchemaRuleNotFoundException

    {
        when( tokenRead.nodeLabel( anyString() ) ).thenReturn( 0 );
        when( tokenRead.propertyKey( anyString() ) ).thenReturn( 0 );
        when( schemaRead.index( anyInt(), any() ) ).thenReturn( CapableIndexReference.NO_INDEX );

        try
        {
            procedure.resampleIndex( ":Person(name)" );
            fail( "Expected an exception" );
        }
        catch ( ProcedureException e )
        {
            assertThat( e.status(), is( Status.Schema.IndexNotFound ) );
        }
    }

    @Test
    public void shouldTriggerResampling()
            throws SchemaRuleNotFoundException, ProcedureException, IndexNotFoundKernelException
    {
        SchemaIndexDescriptor index = SchemaIndexDescriptorFactory.forLabel( 123, 456 );
        when( schemaRead.index( anyInt(), any() ) ).thenReturn( fromDescriptor( index ) );

        procedure.resampleIndex( ":Person(name)" );

        verify( indexingService ).triggerIndexSampling( index.schema(), IndexSamplingMode.TRIGGER_REBUILD_ALL );
    }
}

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
package org.neo4j.kernel.builtinprocs;

import org.junit.Test;

import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.storageengine.api.schema.SchemaRule.Kind.INDEX_RULE;

public class ResampleIndexProcedureTest
{
    private final ReadOperations operations = mock( ReadOperations.class );
    private final IndexingService indexingService = mock( IndexingService.class );
    private final IndexProcedures procedure =
            new IndexProcedures( new StubKernelTransaction( operations ), indexingService );

    @Test
    public void shouldThrowAnExceptionIfTheLabelDoesntExist() throws ProcedureException
    {
        when( operations.labelGetForName( "NonExistentLabel" ) ).thenReturn( -1 );

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
    public void shouldThrowAnExceptionIfThePropertyKeyDoesntExist() throws ProcedureException
    {
        when( operations.propertyKeyGetForName( "nonExistentProperty" ) ).thenReturn( -1 );

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
            throws ProcedureException, SchemaRuleNotFoundException, IndexNotFoundKernelException
    {
        IndexDescriptor index = IndexDescriptorFactory.forLabel( 0, 0 );
        when( operations.labelGetForName( anyString() ) ).thenReturn( 123 );
        when( operations.propertyKeyGetForName( anyString() ) ).thenReturn( 456 );
        when( operations.indexGetForSchema( any() ) ).thenReturn( index );

        procedure.resampleIndex( ":Person(name)" );

        verify( operations ).indexGetForSchema( SchemaDescriptorFactory.forLabel( 123, 456 ) );
    }

    @Test
    public void shouldLookUpTheCompositeIndexByLabelIdAndPropertyKeyId()
            throws ProcedureException, SchemaRuleNotFoundException, IndexNotFoundKernelException
    {
        IndexDescriptor index = IndexDescriptorFactory.forLabel( 0, 0, 1 );
        when( operations.labelGetForName( anyString() ) ).thenReturn( 123 );
        when( operations.propertyKeyGetForName( "name" ) ).thenReturn( 0 );
        when( operations.propertyKeyGetForName( "lastName" ) ).thenReturn( 1 );
        when( operations.indexGetForSchema( SchemaDescriptorFactory.forLabel( 123, 0, 1 ) ) )
                .thenReturn( index );

        procedure.resampleIndex( ":Person(name, lastName)" );

        verify( operations ).indexGetForSchema( SchemaDescriptorFactory.forLabel( 123, 0, 1 ) );
    }

    @Test
    public void shouldThrowAnExceptionIfTheIndexDoesNotExist()
            throws SchemaRuleNotFoundException, IndexNotFoundKernelException

    {
        when( operations.labelGetForName( anyString() ) ).thenReturn( 0 );
        when( operations.propertyKeyGetForName( anyString() ) ).thenReturn( 0 );
        when( operations.indexGetForSchema( any() ) ).thenThrow(
                new SchemaRuleNotFoundException( INDEX_RULE, SchemaDescriptorFactory.forLabel( 0, 0 ) ) );

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
        IndexDescriptor index = IndexDescriptorFactory.forLabel( 123, 456 );
        when( operations.indexGetForSchema( any() ) ).thenReturn( index );

        procedure.resampleIndex( ":Person(name)" );

        verify( indexingService ).triggerIndexSampling( index.schema(), IndexSamplingMode.TRIGGER_REBUILD_ALL );
    }
}

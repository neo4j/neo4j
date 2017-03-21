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
package org.neo4j.kernel.impl.api;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.exceptions.schema.DuplicateSchemaRuleException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.schema_new.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema_new.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptor;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptorFactory;
import org.neo4j.kernel.impl.api.operations.SchemaReadOperations;
import org.neo4j.test.mockito.matcher.KernelExceptionUserMessageMatcher;

import static org.mockito.Matchers.anyInt;

@RunWith( MockitoJUnitRunner.class )
public class OperationsFacadeTest
{
    private final String LABEL1 = "Label1";
    private final String PROP1 = "Prop1";
    private final LabelSchemaDescriptor descriptor = SchemaDescriptorFactory.forLabel( 1, 2 );

    @Mock
    private KernelStatement kernelStatement;
    @Mock
    private StatementOperationParts statementOperationParts;
    @Mock
    private SchemaReadOperations schemaReadOperations;
    @InjectMocks
    private OperationsFacade operationsFacade;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() throws Exception
    {
        operationsFacade.initialize( statementOperationParts );
    }

    @Test
    public void testThrowExceptionWhenIndexNotFoundByLabelAndProperty() throws SchemaRuleNotFoundException
    {
        setupSchemaReadOperations();

        TokenNameLookup tokenNameLookup = getDefaultTokenNameLookup();

        expectedException.expect( SchemaRuleNotFoundException.class );
        expectedException.expect( new KernelExceptionUserMessageMatcher<>( tokenNameLookup,
                "No index was found for :Label1(Prop1)." ) );

        operationsFacade.indexGetForSchema( descriptor );
    }

    @Test
    public void testThrowExceptionWhenDuplicateUniqueIndexFound()
            throws SchemaRuleNotFoundException, DuplicateSchemaRuleException
    {
        SchemaReadOperations readOperations = setupSchemaReadOperations();
        NewIndexDescriptor index = NewIndexDescriptorFactory.forSchema( descriptor );
        Mockito.when( readOperations
                .uniqueIndexesGetForLabel( Mockito.any( KernelStatement.class ), Mockito.eq( descriptor.getLabelId() ) ) )
                .thenReturn( Iterators.iterator( index, index ) );
        TokenNameLookup tokenNameLookup = getDefaultTokenNameLookup();

        expectedException.expect( DuplicateSchemaRuleException.class );
        expectedException.expect( new KernelExceptionUserMessageMatcher<>( tokenNameLookup,
                "Multiple constraint indexs found for :Label1(Prop1)." ) );

        operationsFacade.uniqueIndexGetForLabelAndPropertyKey( descriptor );
    }

    @Test
    public void testThrowExceptionWhenUniqueIndexNotFound()
            throws SchemaRuleNotFoundException, DuplicateSchemaRuleException
    {
        SchemaReadOperations readOperations = setupSchemaReadOperations();
        TokenNameLookup tokenNameLookup = getDefaultTokenNameLookup();
        Mockito.when( readOperations
                .uniqueIndexesGetForLabel( Mockito.any( KernelStatement.class ), anyInt() ) )
                .thenReturn( Iterators.emptyIterator() );

        expectedException.expect( SchemaRuleNotFoundException.class );
        expectedException.expect( new KernelExceptionUserMessageMatcher<>( tokenNameLookup,
                "No constraint index was found for :Label1(Prop1)." ) );

        operationsFacade.uniqueIndexGetForLabelAndPropertyKey( descriptor );
    }

    private SchemaReadOperations setupSchemaReadOperations()
    {
        SchemaReadOperations readOperations = Mockito.mock(SchemaReadOperations.class);
        Mockito.when( statementOperationParts.schemaReadOperations() ).thenReturn( readOperations );
        return readOperations;
    }

    private TokenNameLookup getDefaultTokenNameLookup()
    {
        TokenNameLookup tokenNameLookup = Mockito.mock( TokenNameLookup.class );
        Mockito.when( tokenNameLookup.labelGetName( descriptor.getLabelId() ) ).thenReturn( LABEL1 );
        Mockito.when( tokenNameLookup.propertyKeyGetName( descriptor.getPropertyId() ) ).thenReturn( PROP1 );
        return tokenNameLookup;
    }

}

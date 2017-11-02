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

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class OperationsFacadeTest
{
    @Rule
    public DatabaseRule db = new ImpermanentDatabaseRule();

    private final Label LABEL1 = Label.label( "Label1" );
    private final String PROP1 = "Prop1";
    private int labelId;
    private int propertyId;

    @Before
    public void setup()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( LABEL1 ).setProperty( PROP1, 1 );
            tx.success();
        }

        try ( Transaction tx = db.beginTx();
              Statement statement = db.statement() )
        {
            ReadOperations readOperations = statement.readOperations();
            labelId = readOperations.labelGetForName( LABEL1.name() );
            propertyId = readOperations.propertyKeyGetForName( PROP1 );
            tx.success();
        }
    }

    @Test
    public void testThrowExceptionWhenIndexNotFound() throws SchemaRuleNotFoundException
    {
        try ( Transaction ignored = db.beginTx();
              Statement statement = db.statement() )
        {
            ReadOperations readOperations = statement.readOperations();
            try
            {
                LabelSchemaDescriptor descriptor = SchemaDescriptorFactory.forLabel( 1, 2 );
                readOperations.indexGetForSchema( descriptor );
                fail( "Should have failed" );
            }
            catch ( SchemaRuleNotFoundException e )
            {
                assertThat( e.getMessage(), containsString( "No index was found for :label[1](property[2])" ) );
            }
        }
    }

    @Test
    public void indexGetProviderDescriptorMustReturnUndecidedIfIndexCreatedInTransaction() throws Exception
    {
        try ( Transaction tx = db.beginTx();
              Statement statement = db.statement() )
        {
            db.schema().indexFor( LABEL1 ).on( PROP1 ).create();
            ReadOperations readOperations = statement.readOperations();
            IndexDescriptor indexDescriptor = IndexDescriptorFactory.forLabel( labelId, propertyId );
            SchemaIndexProvider.Descriptor providerDescriptor = readOperations.indexGetProviderDescriptor( indexDescriptor );
            assertThat( providerDescriptor, is( SchemaIndexProvider.UNDECIDED ) );
            tx.success();
        }
    }

    @Test
    public void indexGetProviderDescriptorMustThrowIfIndexDoesNotExist() throws Exception
    {
        try ( Transaction tx = db.beginTx();
              Statement statement = db.statement() )
        {
            ReadOperations readOperations = statement.readOperations();
            IndexDescriptor indexDescriptor = IndexDescriptorFactory.forLabel( labelId, propertyId );

            try
            {
                readOperations.indexGetProviderDescriptor( indexDescriptor );
                fail( "Should have failed" );
            }
            catch ( IndexNotFoundKernelException e )
            {
                // good
                assertThat( e.getMessage(), allOf(
                        containsString( "No index" ),
                        containsString( ":label[" + labelId + "](property[" + propertyId + "])" ) ) );
            }
            tx.success();
        }
    }
}

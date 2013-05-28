/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Function;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.TransactionContext;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.SchemaIndexTestHelper;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.test.ImpermanentDatabaseRule;

import static org.junit.Assert.assertEquals;

public class KernelSchemaStateFlushingTest
{
    public @Rule ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();

    private GraphDatabaseAPI db;
    private AbstractTransactionManager txManager;

    @Test
    public void shouldKeepSchemaStateIfSchemaIsNotModified() throws TransactionFailureException
    {
        // given
        String before = commitToSchemaState( "test", "before" );

        // then
        assertEquals( "before", before );

        // given
        String after = commitToSchemaState( "test", "after" );

        // then
        assertEquals( "before", after );
    }

    @Test
    public void shouldInvalidateSchemaStateOnCreateIndex() throws Exception
    {
        // given
        commitToSchemaState( "test", "before" );
        
        IndexDescriptor descriptor = createIndex();

        awatIndexOnline( descriptor );
        
        // when
        String after = commitToSchemaState( "test", "after" );

        // then
        assertEquals( "after", after );
    }

    @Test
    public void shouldInvalidateSchemaStateOnDropIndex() throws Exception
    {
        IndexDescriptor descriptor = createIndex();

        awatIndexOnline( descriptor );

        commitToSchemaState( "test", "before" );

        dropIndex( descriptor );

        // when
        String after = commitToSchemaState( "test", "after" );

        // then
        assertEquals( "after", after );
    }

    @Test
    public void shouldInvalidateSchemaStateOnCreateConstraint() throws Exception
    {
        // given
        commitToSchemaState( "test", "before" );

        createConstraint();

        // when
        String after = commitToSchemaState( "test", "after" );

        // then
        assertEquals( "after", after );
    }

    @Test
    public void shouldInvalidateSchemaStateOnDropConstraint() throws Exception
    {
        // given
        UniquenessConstraint descriptor = createConstraint();

        commitToSchemaState( "test", "before" );

        dropConstraint( descriptor );

        // when
        String after = commitToSchemaState( "test", "after" );

        // then
        assertEquals( "after", after );
    }

    private UniquenessConstraint createConstraint() throws SchemaKernelException
    {
        Transaction tx = db.beginTx();
        TransactionContext txc = txManager.getTransactionContext();
        StatementContext ctx = txc.newStatementContext();
        UniquenessConstraint descriptor = ctx.uniquenessConstraintCreate( 1, 1 );
        ctx.close();
        tx.success();
        tx.finish();
        return descriptor;
    }

    private void dropConstraint( UniquenessConstraint descriptor )
    {
        Transaction tx = db.beginTx();
        TransactionContext txc = txManager.getTransactionContext();
        StatementContext ctx = txc.newStatementContext();
        ctx.constraintDrop( descriptor );
        ctx.close();
        tx.success();
        tx.finish();
    }

    private IndexDescriptor createIndex() throws SchemaKernelException
    {
        Transaction tx = db.beginTx();
        TransactionContext txc = txManager.getTransactionContext();
        StatementContext ctx = txc.newStatementContext();
        IndexDescriptor descriptor = ctx.indexCreate( 1, 1 );
        ctx.close();
        tx.success();
        tx.finish();
        return descriptor;
    }

    private void dropIndex( IndexDescriptor descriptor ) throws SchemaKernelException
    {
        Transaction tx = db.beginTx();
        TransactionContext txc = txManager.getTransactionContext();
        StatementContext ctx = txc.newStatementContext();
        ctx.indexDrop( descriptor );
        ctx.close();
        tx.success();
        tx.finish();
    }

    private void awatIndexOnline( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        Transaction tx = db.beginTx();
        TransactionContext txc = txManager.getTransactionContext();
        StatementContext ctx = txc.newStatementContext();
        SchemaIndexTestHelper.awaitIndexOnline( ctx, descriptor );
        ctx.close();
        tx.success();
        tx.finish();
    }

    private String commitToSchemaState( String key, String value ) throws TransactionFailureException
    {
        Transaction tx = db.beginTx();
        TransactionContext txc = txManager.getTransactionContext();
        String result;
        try 
        {
            result = getOrCreateFromState( txc, key, value );
            return result;            
        }
        finally
        {
            tx.success();
            tx.finish();
        }
    }
    
    private String getOrCreateFromState( TransactionContext tx, String key, final String value )
    {
        StatementContext ctx = tx.newStatementContext();
        try 
        {
            return ctx.schemaStateGetOrCreate( key, new Function<String, String>()
            {
                @Override
                public String apply( String from )
                {
                    return value;
                }
            } );
        }
        finally 
        {
            ctx.close();
        }
    }
    
    @Before
    public void setup() 
    {
        db = dbRule.getGraphDatabaseAPI();         
        txManager = db.getDependencyResolver().resolveDependency( AbstractTransactionManager.class );
    }
    
    @After
    public void afer()
    {
        db.shutdown();
    }

}

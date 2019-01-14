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
package org.neo4j.kernel.impl.api;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.locks.LockSupport;

import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.Session;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.impl.api.index.SchemaIndexTestHelper;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;

public class KernelSchemaStateFlushingTest
{
    @Rule
    public ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();

    private GraphDatabaseAPI db;
    private Kernel kernel;
    private Session session;

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

        awaitIndexOnline( createIndex(), "test" );

        // when
        String after = commitToSchemaState( "test", "after" );

        // then
        assertEquals( "after", after );
    }

    @Test
    public void shouldInvalidateSchemaStateOnDropIndex() throws Exception
    {
        IndexReference ref = createIndex();

        awaitIndexOnline( ref, "test" );

        commitToSchemaState( "test", "before" );

        dropIndex( ref );

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
        ConstraintDescriptor descriptor = createConstraint();

        commitToSchemaState( "test", "before" );

        dropConstraint( descriptor );

        // when
        String after = commitToSchemaState( "test", "after" );

        // then
        assertEquals( "after", after );
    }

    private ConstraintDescriptor createConstraint() throws KernelException
    {

        try ( Transaction transaction = session.beginTransaction( KernelTransaction.Type.implicit ) )
        {
            ConstraintDescriptor descriptor = transaction.schemaWrite().uniquePropertyConstraintCreate(
                    SchemaDescriptorFactory.forLabel( 1, 1 ) );
            transaction.success();
            return descriptor;
        }
    }

    private void dropConstraint( ConstraintDescriptor descriptor ) throws KernelException
    {
        try ( Transaction transaction = session.beginTransaction( KernelTransaction.Type.implicit ) )
        {
            transaction.schemaWrite().constraintDrop( descriptor );
            transaction.success();
        }
    }

    private IndexReference createIndex() throws KernelException
    {
        try ( Transaction transaction = session.beginTransaction( KernelTransaction.Type.implicit ) )
        {
            IndexReference reference = transaction.schemaWrite().indexCreate(
                    SchemaDescriptorFactory.forLabel( 1, 1 ), null );
            transaction.success();
            return reference;
        }
    }

    private void dropIndex( IndexReference reference ) throws KernelException
    {
        try ( Transaction transaction = session.beginTransaction( KernelTransaction.Type.implicit ) )
        {
            transaction.schemaWrite().indexDrop( reference );
            transaction.success();
        }
    }

    private void awaitIndexOnline( IndexReference descriptor, String keyForProbing )
            throws IndexNotFoundKernelException, TransactionFailureException
    {
        try ( Transaction transaction = session.beginTransaction( KernelTransaction.Type.implicit ) )
        {
            SchemaIndexTestHelper.awaitIndexOnline( transaction.schemaRead(), descriptor );
            transaction.success();
        }
        awaitSchemaStateCleared( keyForProbing );
    }

    private void awaitSchemaStateCleared( String keyForProbing ) throws TransactionFailureException
    {
        try ( Transaction transaction = session.beginTransaction( KernelTransaction.Type.implicit ) )
        {
            while ( transaction.schemaRead().schemaStateGetOrCreate( keyForProbing, ignored -> null ) != null )
            {
                LockSupport.parkNanos( MILLISECONDS.toNanos( 10 ) );
            }
            transaction.success();
        }
    }

    private String commitToSchemaState( String key, String value ) throws TransactionFailureException
    {
        try ( Transaction transaction = session.beginTransaction( KernelTransaction.Type.implicit ) )
        {
            String result = getOrCreateFromState( transaction, key, value );
            transaction.success();
            return result;
        }
    }

    private String getOrCreateFromState( Transaction tx, String key, final String value )
    {
        return tx.schemaRead().schemaStateGetOrCreate( key, from -> value );
    }

    @Before
    public void setup()
    {
        db = dbRule.getGraphDatabaseAPI();
        kernel = db.getDependencyResolver().resolveDependency( Kernel.class );
        session = kernel.beginSession( AUTH_DISABLED );
    }

    @After
    public void after()
    {
        session.close();
        db.shutdown();
    }
}

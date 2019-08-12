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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.locks.LockSupport;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.impl.api.index.SchemaIndexTestHelper;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.kernel.api.Transaction.Type.implicit;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;

@ImpermanentDbmsExtension
class KernelSchemaStateFlushingTest
{
    @Inject
    private GraphDatabaseAPI db;

    private Kernel kernel;

    @BeforeEach
    void setup() throws KernelException
    {
        kernel = db.getDependencyResolver().resolveDependency( Kernel.class );
        try ( Transaction transaction = beginTransaction() )
        {
            // Make sure that a label token with id 1, and a property key token, also with id 1, both exists.
            transaction.tokenWrite().labelGetOrCreateForName( "Label0" );
            transaction.tokenWrite().labelGetOrCreateForName( "Label1" );
            transaction.tokenWrite().propertyKeyGetOrCreateForName( "prop0" );
            transaction.tokenWrite().propertyKeyGetOrCreateForName( "prop1" );
            transaction.commit();
        }
    }

    @Test
    void shouldKeepSchemaStateIfSchemaIsNotModified() throws TransactionFailureException
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
    void shouldInvalidateSchemaStateOnCreateIndex() throws Exception
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
    void shouldInvalidateSchemaStateOnDropIndex() throws Exception
    {
        IndexDescriptor ref = createIndex();

        awaitIndexOnline( ref, "test" );

        commitToSchemaState( "test", "before" );

        dropIndex( ref );

        // when
        String after = commitToSchemaState( "test", "after" );

        // then
        assertEquals( "after", after );
    }

    @Test
    void shouldInvalidateSchemaStateOnCreateConstraint() throws Exception
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
    void shouldInvalidateSchemaStateOnDropConstraint() throws Exception
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
        try ( Transaction transaction = beginTransaction() )
        {
            ConstraintDescriptor descriptor = transaction.schemaWrite().uniquePropertyConstraintCreate(
                    SchemaDescriptor.forLabel( 1, 1 ), null );
            transaction.commit();
            return descriptor;
        }
    }

    private void dropConstraint( ConstraintDescriptor descriptor ) throws KernelException
    {
        try ( Transaction transaction = beginTransaction() )
        {
            transaction.schemaWrite().constraintDrop( descriptor );
            transaction.commit();
        }
    }

    private IndexDescriptor createIndex() throws KernelException
    {
        try ( Transaction transaction = beginTransaction() )
        {
            IndexDescriptor reference = transaction.schemaWrite().indexCreate(
                    SchemaDescriptor.forLabel( 1, 1 ) );
            transaction.commit();
            return reference;
        }
    }

    private void dropIndex( IndexDescriptor reference ) throws KernelException
    {
        try ( Transaction transaction = beginTransaction() )
        {
            transaction.schemaWrite().indexDrop( reference );
            transaction.commit();
        }
    }

    private void awaitIndexOnline( IndexDescriptor descriptor, String keyForProbing )
            throws IndexNotFoundKernelException, TransactionFailureException
    {
        try ( Transaction transaction = beginTransaction() )
        {
            SchemaIndexTestHelper.awaitIndexOnline( transaction.schemaRead(), descriptor );
            transaction.commit();
        }
        awaitSchemaStateCleared( keyForProbing );
    }

    private void awaitSchemaStateCleared( String keyForProbing ) throws TransactionFailureException
    {
        try ( Transaction transaction = beginTransaction() )
        {
            while ( transaction.schemaRead().schemaStateGetOrCreate( keyForProbing, ignored -> null ) != null )
            {
                LockSupport.parkNanos( MILLISECONDS.toNanos( 10 ) );
            }
            transaction.commit();
        }
    }

    private String commitToSchemaState( String key, String value ) throws TransactionFailureException
    {
        try ( Transaction transaction = beginTransaction() )
        {
            String result = getOrCreateFromState( transaction, key, value );
            transaction.commit();
            return result;
        }
    }

    private String getOrCreateFromState( Transaction tx, String key, final String value )
    {
        return tx.schemaRead().schemaStateGetOrCreate( key, from -> value );
    }

    private Transaction beginTransaction() throws TransactionFailureException
    {
        return kernel.beginTransaction( implicit, AUTH_DISABLED );
    }

}

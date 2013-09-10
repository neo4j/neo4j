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
package org.neo4j.kernel.impl.api.integrationtest;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.ConstraintCreationException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.index.PreexistingIndexEntryConflictException;
import org.neo4j.kernel.impl.api.constraints.ConstraintVerificationFailedKernelException;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

public class UniquenessConstraintVerificationIT extends KernelIntegrationTest
{
    @Test
    public void shouldAbortConstraintCreationWhenDuplicatesExist() throws Exception
    {
        // given
        long node1, node2, foo, name;
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();
            // name is not unique for Foo in the existing data
            Node node = db.createNode( label( "Foo" ) );
            node1 = node.getId();
            node.setProperty( "name", "foo" );
            node = db.createNode( label( "Foo" ) );
            node2 = node.getId();
            node.setProperty( "name", "foo" );
            foo = statement.labelGetForName( "Foo" );
            name = statement.propertyKeyGetForName( "name" );
            commit();
        }

        // when
        try
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            statement.uniquenessConstraintCreate( foo, name );

            fail( "expected exception" );
        }
        // then
        catch ( CreateConstraintFailureException ex )
        {
            assertEquals( new UniquenessConstraint( foo, name ), ex.constraint() );
            Throwable cause = ex.getCause();
            assertThat( cause, instanceOf( ConstraintVerificationFailedKernelException.class ) );
            assertEquals( asSet( new ConstraintVerificationFailedKernelException.Evidence(
                    new PreexistingIndexEntryConflictException( "foo", node1, node2 ) ) ),
                          ((ConstraintVerificationFailedKernelException) cause).evidence() );
        }
    }

    @Test
    public void shouldFailOnCommitIfConstraintIsBrokenAfterConstraintAddedButBeforeConstraintCommitted()
            throws Exception
    {
        // given
        long node1, foo, name;
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();
            Node node = db.createNode( label( "Foo" ) );
            node1 = node.getId();
            node.setProperty( "name", "foo" );
            foo = statement.labelGetForName( "Foo" );
            name = statement.propertyKeyGetForName( "name" );
            commit();
        }

        SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
        statement.uniquenessConstraintCreate(  foo, name );
        ExecutorService executor = Executors.newSingleThreadExecutor();
        long node2 = executor.submit( new Callable<Long>()
        {
            @Override
            public Long call()
            {
                Transaction tx = db.beginTx();
                try
                {
                    Node node = db.createNode( label( "Foo" ) );
                    node.setProperty( "name", "foo" );
                    tx.success();
                    return node.getId();
                }
                finally
                {
                    tx.finish();
                }
            }
        } ).get();
        executor.shutdown();

        // when
        try
        {
            commit();

            fail( "expected exception" );
        }
        // then
        catch ( TransactionFailureException ex )
        {
            Throwable cause = ex.getCause();
            assertThat( cause, instanceOf( ConstraintCreationException.class ) );
            CreateConstraintFailureException creationException = (CreateConstraintFailureException) cause.getCause();

            assertEquals( new UniquenessConstraint( foo, name ), creationException.constraint() );
            cause = creationException.getCause();
            assertThat( cause, instanceOf( ConstraintVerificationFailedKernelException.class ) );
            assertEquals( asSet( new ConstraintVerificationFailedKernelException.Evidence(
                    new PreexistingIndexEntryConflictException( "foo", node1, node2 ) ) ),
                          ((ConstraintVerificationFailedKernelException) cause).evidence() );
        }
    }
}

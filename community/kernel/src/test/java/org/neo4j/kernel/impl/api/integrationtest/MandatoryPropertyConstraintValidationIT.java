/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.junit.Test;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.exceptions.ConstraintViolationTransactionFailureException;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.properties.Property;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class MandatoryPropertyConstraintValidationIT extends KernelIntegrationTest
{
    @Test
    public void shouldEnforceMandatoryConstraintWhenCreatingNodeWithoutProperty() throws Exception
    {
        // given
        createConstraint( "Label1", "key1" );

        DataWriteOperations statement = dataWriteOperationsInNewTransaction();

        // when
       createLabeledNode( statement, "Label1" );
        try
        {
            commit();
            fail( "should have thrown exception" );
        }
        // then
        catch ( ConstraintViolationTransactionFailureException e )
        {
            Status expected = Status.Schema.ConstraintViolation;
            assertThat( e.status(), is( expected ) );
        }
    }

    @Test
    public void shouldEnforceConstraintWhenRemoving() throws Exception
    {
        // given
        long node = constrainedNode( "Label1", "key1", "value1" );
        DataWriteOperations statement = dataWriteOperationsInNewTransaction();

        // when
        int key = statement.propertyKeyGetOrCreateForName( "key1" );
        statement.nodeRemoveProperty( node, key );
        try
        {
            commit();
            fail( "should have thrown exception" );
        }
        // then
        catch ( ConstraintViolationTransactionFailureException e )
        {
            Status expected = Status.Schema.ConstraintViolation;
            assertThat( e.status(), is( expected ) );
        }
    }

    @Test
    public void shouldAllowTemporaryViolationsWithinTransactions() throws Exception
    {
        // given
        long node = constrainedNode( "Label1", "key1", "value1" );
        DataWriteOperations statement = dataWriteOperationsInNewTransaction();

        // when
        int key = statement.propertyKeyGetOrCreateForName( "key1" );
        //remove and put back
        statement.nodeRemoveProperty( node, key );
        statement.nodeSetProperty( node, Property.property( key, "value2" ) );

        commit();
    }

    @Test
    public void shouldAllowNoopPropertyUpdate() throws KernelException
    {
        // given
        long node = constrainedNode( "Label1", "key1", "value1" );

        DataWriteOperations statement = dataWriteOperationsInNewTransaction();

        // when
        statement.nodeSetProperty( node, Property.property(
                statement.propertyKeyGetOrCreateForName( "key1" ), "value1" ) );

        // then should not throw exception
    }

    @Test
    public void shouldAllowNoopLabelUpdate() throws KernelException
    {
        // given
        long node = constrainedNode( "Label1", "key1", "value1" );

        DataWriteOperations statement = dataWriteOperationsInNewTransaction();

        // when
        statement.nodeAddLabel( node, statement.labelGetOrCreateForName( "Label1" ) );

        // then should not throw exception
    }

    @Test
    public void shouldAllowCreationOfNonConflictingData() throws Exception
    {
        // given
        constrainedNode( "Label1", "key1", "value1" );

        DataWriteOperations statement = dataWriteOperationsInNewTransaction();

        // when
        createNode( statement, "key1", "value1" );
        createLabeledNode( statement, "Label2");
        createLabeledNode( statement, "Label1", "key1", "value2" );
        createLabeledNode( statement, "Label1", "key1", "value1" );

        commit();

        // then
        statement = dataWriteOperationsInNewTransaction();
        assertEquals( "number of nodes", 5, PrimitiveLongCollections.count( statement.nodesGetAll() ) );
        rollback();
    }

    private long createNode( DataWriteOperations statement, String key, Object value ) throws KernelException
    {
        long node = statement.nodeCreate();
        statement.nodeSetProperty( node, Property.property(
                statement.propertyKeyGetOrCreateForName( key ), value ) );
        return node;
    }

    private long createLabeledNode( DataWriteOperations statement, String label )
            throws KernelException
    {
        long node = statement.nodeCreate();
        statement.nodeAddLabel( node, statement.labelGetOrCreateForName( label ) );
        return node;
    }

    private long createLabeledNode( DataWriteOperations statement, String label, String key, Object value )
            throws KernelException
    {
        long node = createLabeledNode( statement, label );
        statement.nodeSetProperty( node, Property.property(
                statement.propertyKeyGetOrCreateForName( key ), value ) );
        return node;
    }

    private long constrainedNode( String labelName, String propertyKey, Object propertyValue )
            throws KernelException
    {
        long node;
        {
            DataWriteOperations dataStatement = dataWriteOperationsInNewTransaction();
            int label = dataStatement.labelGetOrCreateForName( labelName );
            node = dataStatement.nodeCreate();
            dataStatement.nodeAddLabel( node, label );
            int key = dataStatement.propertyKeyGetOrCreateForName( propertyKey );
            dataStatement.nodeSetProperty( node, Property.property( key, propertyValue ) );
            commit();
        }
        createConstraint( labelName, propertyKey );
        return node;
    }

    private void createConstraint( String label, String propertyKey ) throws KernelException
    {
        int labelId, propertyKeyId;
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();
            labelId = statement.labelGetOrCreateForName( label );
            propertyKeyId = statement.propertyKeyGetOrCreateForName( propertyKey );
            commit();
        }

        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            statement.mandatoryPropertyConstraintCreate( labelId, propertyKeyId );
            commit();
        }
    }
}

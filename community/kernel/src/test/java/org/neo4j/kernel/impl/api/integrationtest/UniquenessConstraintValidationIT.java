/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import org.neo4j.kernel.api.StatementTokenNameLookup;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyConstraintViolationKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.properties.Property;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class UniquenessConstraintValidationIT extends KernelIntegrationTest
{
    @Test
    public void shouldEnforceUniquenessConstraintOnSetProperty() throws Exception
    {
        // given
        constrainedNode( "Label1", "key1", "value1" );

        DataWriteOperations statement = dataWriteOperationsInNewTransaction();

        // when
        long node = createLabeledNode( statement, "Label1" );
        try
        {
            statement.nodeSetProperty( node, Property.property(
                    statement.propertyKeyGetOrCreateForName( "key1" ), "value1" ) );

            fail( "should have thrown exception" );
        }
        // then
        catch ( UniquePropertyConstraintViolationKernelException e )
        {
            assertThat( e.getUserMessage( tokenLookup( statement ) ), containsString( "\"key1\"=[value1]" ) );
        }
    }

    @Test
    public void roundingErrorsFromLongToDoubleShouldNotPreventTxFromCommitting() throws Exception
    {
        // Given
        // a node with a constrained label and a long value
        long propertyValue = 285414114323346805l;
        long firstNode = constrainedNode( "label1", "key1", propertyValue );

        DataWriteOperations statement = dataWriteOperationsInNewTransaction();

        long node = createLabeledNode( statement, "label1" );

        assertNotEquals( firstNode, node );

        // When
        // a new node with the same constraint is added, with a value not equal but which would be mapped to the same double
        propertyValue++;
        // note how propertyValue is definitely not equal to propertyValue++ but they do equal if they are cast to double
        statement.nodeSetProperty( node, Property.property( statement.propertyKeyGetOrCreateForName( "key1" ), propertyValue ) );

        // Then
        // the commit should still succeed
        commit();
    }

    @Test
    public void shouldEnforceUniquenessConstraintOnAddLabelForNumberPropertyOnNodeNotFromTransaction() throws Exception
    {
        // given
        constrainedNode( "Label1", "key1", 1 );

        // when
        DataWriteOperations statement = dataWriteOperationsInNewTransaction();
        long node = createNode( statement, "key1", 1 );
        commit();

        statement = dataWriteOperationsInNewTransaction();
        try
        {
            statement.nodeAddLabel( node, statement.labelGetOrCreateForName( "Label1" ) );

            fail( "should have thrown exception" );
        }
        // then
        catch ( UniquePropertyConstraintViolationKernelException e )
        {
            assertThat( e.getUserMessage( tokenLookup( statement ) ), containsString( "\"key1\"=[1]" ) );
        }
    }

    @Test
    public void shouldEnforceUniquenessConstraintOnAddLabelForStringProperty() throws Exception
    {
        // given
        constrainedNode( "Label1", "key1", "value1" );

        DataWriteOperations statement = dataWriteOperationsInNewTransaction();

        // when
        long node = createNode( statement, "key1", "value1" );
        try
        {
            statement.nodeAddLabel( node, statement.labelGetOrCreateForName( "Label1" ) );

            fail( "should have thrown exception" );
        }
        // then
        catch ( UniquePropertyConstraintViolationKernelException e )
        {
            assertThat( e.getUserMessage( tokenLookup( statement ) ), containsString( "\"key1\"=[value1]" ) );
        }
    }

    @Test
    public void shouldAllowRemoveAndAddConflictingDataInOneTransaction_DeleteNode() throws Exception
    {
        // given
        long node = constrainedNode( "Label1", "key1", "value1" );

        DataWriteOperations statement = dataWriteOperationsInNewTransaction();

        // when
        statement.nodeDelete( node );
        createLabeledNode( statement, "Label1", "key1", "value1" );
        commit();
    }

    private long createLabeledNode( DataWriteOperations statement, String label )
            throws KernelException
    {
        long node = statement.nodeCreate();
        statement.nodeAddLabel( node, statement.labelGetOrCreateForName( label ) );
        return node;
    }

    private long createNode( DataWriteOperations statement, String key, Object value ) throws KernelException
    {
        long node = statement.nodeCreate();
        statement.nodeSetProperty( node, Property.property(
                statement.propertyKeyGetOrCreateForName( key ), value ) );
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

    @Test
    public void shouldAllowRemoveAndAddConflictingDataInOneTransaction_RemoveLabel() throws Exception
    {
        // given
        long node = constrainedNode( "Label1", "key1", "value1" );

        DataWriteOperations statement = dataWriteOperationsInNewTransaction();

        // when
        statement.nodeRemoveLabel( node, statement.labelGetOrCreateForName( "Label1" ) );
        createLabeledNode( statement, "Label1", "key1", "value1" );
        commit();
    }

    @Test
    public void shouldAllowRemoveAndAddConflictingDataInOneTransaction_RemoveProperty() throws Exception
    {
        // given
        long node = constrainedNode( "Label1", "key1", "value1" );

        DataWriteOperations statement = dataWriteOperationsInNewTransaction();

        // when
        statement.nodeRemoveProperty( node, statement.propertyKeyGetForName( "key1" ) );
        createLabeledNode( statement, "Label1", "key1", "value1" );
        commit();
    }

    @Test
    public void shouldAllowRemoveAndAddConflictingDataInOneTransaction_ChangeProperty() throws Exception
    {
        // given
        long node = constrainedNode( "Label1", "key1", "value1" );

        DataWriteOperations statement = dataWriteOperationsInNewTransaction();

        // when
        statement.nodeSetProperty( node, Property.property(
                statement.propertyKeyGetOrCreateForName( "key1" ), "value2" ) );
        createLabeledNode( statement, "Label1", "key1", "value1" );
        commit();
    }

    @Test
    public void shouldPreventConflictingDataInSameTransaction() throws Exception
    {
        // given
        constrainedNode( "Label1", "key1", "value1" );

        DataWriteOperations statement = dataWriteOperationsInNewTransaction();

        // when
        createLabeledNode( statement, "Label1", "key1", "value2" );
        try
        {
            createLabeledNode( statement, "Label1", "key1", "value2" );

            fail( "expected exception" );
        }
        // then
        catch ( UniquePropertyConstraintViolationKernelException e )
        {
            assertThat( e.getUserMessage( tokenLookup( statement ) ), containsString( "\"key1\"=[value2]" ) );
        }
    }

    private TokenNameLookup tokenLookup( DataWriteOperations statement )
    {
        return new StatementTokenNameLookup( statement );
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
        createLabeledNode( statement, "Label2", "key1", "value1" );
        createLabeledNode( statement, "Label1", "key1", "value2" );
        createLabeledNode( statement, "Label1", "key2", "value1" );

        commit();

        // then
        statement = dataWriteOperationsInNewTransaction();
        assertEquals( "number of nodes", 5, PrimitiveLongCollections.count( statement.nodesGetAll() ) );
        rollback();
    }

    @Test
    public void unrelatedNodesWithSamePropertyShouldNotInterfereWithUniquenessCheck() throws Exception
    {
        // given
        createConstraint( "Person", "id" );

        long ourNode;
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();

            ourNode = createLabeledNode( statement, "Person", "id", 1 );
            createLabeledNode( statement, "Item", "id", 2 );

            commit();
        }

        DataWriteOperations statement = dataWriteOperationsInNewTransaction();
        IndexDescriptor idx = statement.uniqueIndexGetForLabelAndPropertyKey( statement
                .labelGetForName( "Person" ), statement.propertyKeyGetForName( "id" ) );

        // when
        createLabeledNode( statement, "Item", "id", 2 );

        // then I should find the original node
        assertThat( statement.nodeGetFromUniqueIndexSeek( idx, 1 ), equalTo( ourNode ) );
    }

    @Test
    public void addingUniqueNodeWithUnrelatedValueShouldNotAffectLookup() throws Exception
    {
        // given
        createConstraint( "Person", "id" );

        long ourNode;
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();

            ourNode = createLabeledNode( statement, "Person", "id", 1 );
            commit();
        }

        DataWriteOperations statement = dataWriteOperationsInNewTransaction();
        IndexDescriptor idx = statement.uniqueIndexGetForLabelAndPropertyKey( statement
                .labelGetForName( "Person" ), statement.propertyKeyGetForName( "id" ) );

        // when
        createLabeledNode( statement, "Person", "id", 2 );

        // then I should find the original node
        assertThat( statement.nodeGetFromUniqueIndexSeek( idx, 1 ), equalTo( ourNode ));
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
            statement.uniquePropertyConstraintCreate( labelId, propertyKeyId );
            commit();
        }
    }
}

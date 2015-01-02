/**
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

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.Iterables.count;

public class UniquenessConstraintValidationIT extends KernelIntegrationTest
{
    @Test
    public void shouldEnforceUniquenessConstraintOnSetProperty() throws Exception
    {
        // given
        constrainedNode( "Label1", "key1", "value1" );

        dataWriteOperationsInNewTransaction();

        // when
        Node node = db.createNode( label( "Label1" ) );
        try
        {
            node.setProperty( "key1", "value1" );

            fail( "should have thrown exception" );
        }
        // then
        catch ( ConstraintViolationException e )
        {
            assertThat( e.getMessage(), containsString( "\"key1\"=[value1]" ) );
        }
    }

    @Test
    public void shouldEnforceUniquenessConstraintOnAddLabel() throws Exception
    {
        // given
        constrainedNode( "Label1", "key1", "value1" );

        dataWriteOperationsInNewTransaction();

        // when
        Node node = db.createNode();
        node.setProperty( "key1", "value1" );
        try
        {
            node.addLabel( label( "Label1" ) );

            fail( "should have thrown exception" );
        }
        // then
        catch ( ConstraintViolationException e )
        {
            assertThat( e.getMessage(), containsString( "\"key1\"=[value1]" ) );
        }
    }

    @Test
    public void shouldAllowRemoveAndAddConflictingDataInOneTransaction_DeleteNode() throws Exception
    {
        // given
        Node node = constrainedNode( "Label1", "key1", "value1" );

        dataWriteOperationsInNewTransaction();

        // when
        node.delete();
        db.createNode( label( "Label1" ) ).setProperty( "key1", "value1" );
        commit();
    }

    @Test
    public void shouldAllowRemoveAndAddConflictingDataInOneTransaction_RemoveLabel() throws Exception
    {
        // given
        Node node = constrainedNode( "Label1", "key1", "value1" );

        dataWriteOperationsInNewTransaction();

        // when
        node.removeLabel( label( "Label1" ) );
        db.createNode( label( "Label1" ) ).setProperty( "key1", "value1" );
        commit();
    }

    @Test
    public void shouldAllowRemoveAndAddConflictingDataInOneTransaction_RemoveProperty() throws Exception
    {
        // given
        Node node = constrainedNode( "Label1", "key1", "value1" );

        dataWriteOperationsInNewTransaction();

        // when
        node.removeProperty( "key1" );
        db.createNode( label( "Label1" ) ).setProperty( "key1", "value1" );
        commit();
    }

    @Test
    public void shouldAllowRemoveAndAddConflictingDataInOneTransaction_ChangeProperty() throws Exception
    {
        // given
        Node node = constrainedNode( "Label1", "key1", "value1" );

        dataWriteOperationsInNewTransaction();

        // when
        node.setProperty( "key1", "value2" );
        db.createNode( label( "Label1" ) ).setProperty( "key1", "value1" );
        commit();
    }

    @Test
    public void shouldPreventConflictingDataInSameTransaction() throws Exception
    {
        // given
        constrainedNode( "Label1", "key1", "value1" );

        dataWriteOperationsInNewTransaction();

        // when
        db.createNode( label( "Label1" ) ).setProperty( "key1", "value2" );
        try
        {
            db.createNode( label( "Label1" ) ).setProperty( "key1", "value2" );

            fail( "expected exception" );
        }
        // then
        catch ( ConstraintViolationException e )
        {
            assertThat( e.getMessage(), containsString( "\"key1\"=[value2]" ) );
        }
    }

    @Test
    public void shouldAllowNoopPropertyUpdate() throws KernelException
    {
        // given
        Node node = constrainedNode( "Label1", "key1", "value1" );

        dataWriteOperationsInNewTransaction();

        // when
        node.setProperty( "key1", "value1" );

        // then should not throw exception
    }

    @Test
    public void shouldAllowNoopLabelUpdate() throws KernelException
    {
        // given
        Node node = constrainedNode( "Label1", "key1", "value1" );

        dataWriteOperationsInNewTransaction();

        // when
        node.addLabel( label( "Label1" ) );

        // then should not throw exception
    }

    @Test
    public void shouldAllowCreationOfNonConflictingData() throws Exception
    {
        // given
        constrainedNode( "Label1", "key1", "value1" );

        dataWriteOperationsInNewTransaction();

        // when
        db.createNode().setProperty( "key1", "value1" );
        db.createNode( label( "Label2" ) ).setProperty( "key1", "value1" );
        db.createNode( label( "Label1" ) ).setProperty( "key1", "value2" );
        db.createNode( label( "Label1" ) ).setProperty( "key2", "value1" );

        commit();

        // then
        dataWriteOperationsInNewTransaction();
        assertEquals( "number of nodes", 5, count( GlobalGraphOperations.at( db ).getAllNodes() ) );
        rollback();
    }

    @Test
    public void unrelatedNodesWithSamePropertyShouldNotInterfereWithUniquenessCheck() throws Exception
    {
        // given
        createConstraint( "Person", "id" );

        Node ourNode;
        {
            dataWriteOperationsInNewTransaction();

            ourNode = db.createNode( label( "Person" ) );
            ourNode.setProperty( "id", 1 );
            db.createNode( label( "Item" ) ).setProperty( "id", 2 );

            commit();
        }

        DataWriteOperations statement = dataWriteOperationsInNewTransaction();
        IndexDescriptor idx = statement.uniqueIndexGetForLabelAndPropertyKey( statement
                .labelGetForName( "Person" ), statement.propertyKeyGetForName( "id" ) );

        // when
        db.createNode( label( "Item" ) ).setProperty( "id", 2 );

        // then I should find the original node
        assertThat( statement.nodeGetUniqueFromIndexLookup( idx, 1 ), equalTo( ourNode.getId() ));
    }

    @Test
    public void addingUniqueNodeWithUnrelatedValueShouldNotAffectLookup() throws Exception
    {
        // given
        createConstraint( "Person", "id" );

        Node ourNode;
        {
            dataWriteOperationsInNewTransaction();

            ourNode = db.createNode( label( "Person" ) );
            ourNode.setProperty( "id", 1 );
            commit();
        }

        DataWriteOperations statement = dataWriteOperationsInNewTransaction();
        IndexDescriptor idx = statement.uniqueIndexGetForLabelAndPropertyKey( statement
                .labelGetForName( "Person" ), statement.propertyKeyGetForName( "id" ) );

        // when
        db.createNode( label( "Person" ) ).setProperty( "id", 2 );

        // then I should find the original node
        assertThat( statement.nodeGetUniqueFromIndexLookup( idx, 1 ), equalTo( ourNode.getId() ));
    }

    private Node constrainedNode( String labelName, String propertyKey, Object propertyValue )
            throws KernelException
    {
        Node node;
        {
            dataWriteOperationsInNewTransaction();
            node = db.createNode( label( labelName ) );
            node.setProperty( propertyKey, propertyValue );
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
            statement.uniquenessConstraintCreate( labelId, propertyKeyId );
            commit();
        }
    }
}

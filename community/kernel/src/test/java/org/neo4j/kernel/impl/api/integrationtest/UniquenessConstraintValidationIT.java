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
package org.neo4j.kernel.impl.api.integrationtest;

import org.junit.Test;

import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementTokenNameLookup;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.TokenWriteOperations;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.values.storable.Values;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.count;
import static org.neo4j.kernel.api.schema.IndexQuery.exact;
import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.forLabel;

public class UniquenessConstraintValidationIT extends KernelIntegrationTest
{
    @Test
    public void shouldEnforceOnSetProperty() throws Exception
    {
        // given
        constrainedNode( "Label1", "key1", "value1" );

        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );

        // when
        long node = createLabeledNode( statement, "Label1" );
        try
        {
            int propertyKeyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( "key1" );
            statement.dataWriteOperations().nodeSetProperty( node, propertyKeyId, Values.of( "value1" ) );

            fail( "should have thrown exception" );
        }
        // then
        catch ( UniquePropertyValueValidationException e )
        {
            assertThat( e.getUserMessage( tokenLookup( statement ) ), containsString( "`key1` = 'value1'" ) );
        }
        commit();
    }

    @Test
    public void roundingErrorsFromLongToDoubleShouldNotPreventTxFromCommitting() throws Exception
    {
        // Given
        // a node with a constrained label and a long value
        long propertyValue = 285414114323346805L;
        long firstNode = constrainedNode( "label1", "key1", propertyValue );

        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );

        long node = createLabeledNode( statement, "label1" );

        assertNotEquals( firstNode, node );

        // When
        // a new node with the same constraint is added, with a value not equal but which would be mapped to the same double
        propertyValue++;
        // note how propertyValue is definitely not equal to propertyValue++ but they do equal if they are cast to double
        int propertyKeyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( "key1" );
        statement.dataWriteOperations().nodeSetProperty( node, propertyKeyId, Values.of( propertyValue ) );

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
        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );
        long node = createNode( statement, "key1", 1 );
        commit();

        statement = statementInNewTransaction( AnonymousContext.writeToken() );
        try
        {
            int label = statement.tokenWriteOperations().labelGetOrCreateForName( "Label1" );
            statement.dataWriteOperations().nodeAddLabel( node, label );

            fail( "should have thrown exception" );
        }
        // then
        catch ( UniquePropertyValueValidationException e )
        {
            assertThat( e.getUserMessage( tokenLookup( statement ) ), containsString( "`key1` = 1" ) );
        }
        commit();
    }

    @Test
    public void shouldEnforceUniquenessConstraintOnAddLabelForStringProperty() throws Exception
    {
        // given
        constrainedNode( "Label1", "key1", "value1" );

        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );

        // when
        long node = createNode( statement, "key1", "value1" );
        try
        {
            int label = statement.tokenWriteOperations().labelGetOrCreateForName( "Label1" );
            statement.dataWriteOperations().nodeAddLabel( node, label );

            fail( "should have thrown exception" );
        }
        // then
        catch ( UniquePropertyValueValidationException e )
        {
            assertThat( e.getUserMessage( tokenLookup( statement ) ), containsString( "`key1` = 'value1'" ) );
        }
        commit();
    }

    @Test
    public void shouldAllowRemoveAndAddConflictingDataInOneTransaction_DeleteNode() throws Exception
    {
        // given
        long node = constrainedNode( "Label1", "key1", "value1" );

        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );

        // when
        statement.dataWriteOperations().nodeDelete( node );
        createLabeledNode( statement, "Label1", "key1", "value1" );
        commit();
    }

    @Test
    public void shouldAllowRemoveAndAddConflictingDataInOneTransaction_RemoveLabel() throws Exception
    {
        // given
        long node = constrainedNode( "Label1", "key1", "value1" );

        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );

        // when
        int label = statement.tokenWriteOperations().labelGetOrCreateForName( "Label1" );
        statement.dataWriteOperations().nodeRemoveLabel( node, label );
        createLabeledNode( statement, "Label1", "key1", "value1" );
        commit();
    }

    @Test
    public void shouldAllowRemoveAndAddConflictingDataInOneTransaction_RemoveProperty() throws Exception
    {
        // given
        long node = constrainedNode( "Label1", "key1", "value1" );

        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );

        // when
        int key = statement.readOperations().propertyKeyGetForName( "key1" );
        statement.dataWriteOperations().nodeRemoveProperty( node, key );
        createLabeledNode( statement, "Label1", "key1", "value1" );
        commit();
    }

    @Test
    public void shouldAllowRemoveAndAddConflictingDataInOneTransaction_ChangeProperty() throws Exception
    {
        // given
        long node = constrainedNode( "Label1", "key1", "value1" );

        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );

        // when
        int propertyKeyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( "key1" );
        statement.dataWriteOperations().nodeSetProperty( node, propertyKeyId, Values.of( "value2" ) );
        createLabeledNode( statement, "Label1", "key1", "value1" );
        commit();
    }

    @Test
    public void shouldPreventConflictingDataInSameTransaction() throws Exception
    {
        // given
        constrainedNode( "Label1", "key1", "value1" );

        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );

        // when
        createLabeledNode( statement, "Label1", "key1", "value2" );
        try
        {
            createLabeledNode( statement, "Label1", "key1", "value2" );

            fail( "expected exception" );
        }
        // then
        catch ( UniquePropertyValueValidationException e )
        {
            assertThat( e.getUserMessage( tokenLookup( statement ) ), containsString( "`key1` = 'value2'" ) );
        }
        commit();
    }

    @Test
    public void shouldAllowNoopPropertyUpdate() throws KernelException
    {
        // given
        long node = constrainedNode( "Label1", "key1", "value1" );

        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );

        // when
        int key = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( "key1" );
        statement.dataWriteOperations().nodeSetProperty( node, key, Values.of( "value1" ) );

        // then should not throw exception
        commit();
    }

    @Test
    public void shouldAllowNoopLabelUpdate() throws KernelException
    {
        // given
        long node = constrainedNode( "Label1", "key1", "value1" );

        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );

        // when
        int label = statement.tokenWriteOperations().labelGetOrCreateForName( "Label1" );
        statement.dataWriteOperations().nodeAddLabel( node, label );

        // then should not throw exception
        commit();
    }

    @Test
    public void shouldAllowCreationOfNonConflictingData() throws Exception
    {
        // given
        constrainedNode( "Label1", "key1", "value1" );

        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );

        // when
        createNode( statement, "key1", "value1" );
        createLabeledNode( statement, "Label2", "key1", "value1" );
        createLabeledNode( statement, "Label1", "key1", "value2" );
        createLabeledNode( statement, "Label1", "key2", "value1" );

        commit();

        // then
        statement = statementInNewTransaction( AnonymousContext.writeToken() );
        assertEquals( "number of nodes", 5, count( statement.readOperations().nodesGetAll() ) );
        rollback();
    }

    @Test
    public void unrelatedNodesWithSamePropertyShouldNotInterfereWithUniquenessCheck() throws Exception
    {
        // given
        createConstraint( "Person", "id" );

        long ourNode;
        {
            Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );
            ourNode = createLabeledNode( statement, "Person", "id", 1 );
            createLabeledNode( statement, "Item", "id", 2 );
            commit();
        }

        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );
        ReadOperations readOps = statement.readOperations();
        int person = readOps.labelGetForName( "Person" );
        int propId = readOps.propertyKeyGetForName( "id" );
        IndexDescriptor idx = readOps.indexGetForSchema( SchemaDescriptorFactory.forLabel( person, propId ) );

        // when
        createLabeledNode( statement, "Item", "id", 2 );

        // then I should find the original node
        assertThat( readOps.nodeGetFromUniqueIndexSeek( idx, exact( propId, Values.of( 1 ) ) ), equalTo( ourNode ) );
        commit();
    }

    @Test
    public void addingUniqueNodeWithUnrelatedValueShouldNotAffectLookup() throws Exception
    {
        // given
        createConstraint( "Person", "id" );

        long ourNode;
        {
            Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );
            ourNode = createLabeledNode( statement, "Person", "id", 1 );
            commit();
        }

        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );
        ReadOperations readOps = statement.readOperations();
        int person = readOps.labelGetForName( "Person" );
        int propId = readOps.propertyKeyGetForName( "id" );
        IndexDescriptor idx = readOps.indexGetForSchema( SchemaDescriptorFactory.forLabel( person, propId ) );

        // when
        createLabeledNode( statement, "Person", "id", 2 );

        // then I should find the original node
        assertThat( readOps.nodeGetFromUniqueIndexSeek( idx, exact( propId, Values.of( 1 ) ) ), equalTo( ourNode ));
        commit();
    }

    private TokenNameLookup tokenLookup( Statement statement )
    {
        return new StatementTokenNameLookup( statement.readOperations() );
    }

    private long createLabeledNode( Statement statement, String label ) throws KernelException
    {
        long node = statement.dataWriteOperations().nodeCreate();
        int labelId = statement.tokenWriteOperations().labelGetOrCreateForName( label );
        statement.dataWriteOperations().nodeAddLabel( node, labelId );
        return node;
    }

    private long createNode( Statement statement, String key, Object value ) throws KernelException
    {
        long node = statement.dataWriteOperations().nodeCreate();
        int propertyKeyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( key );
        statement.dataWriteOperations().nodeSetProperty( node, propertyKeyId, Values.of( value ) );
        return node;
    }

    private long createLabeledNode( Statement statement, String label, String key, Object value )
            throws KernelException
    {
        long node = createLabeledNode( statement, label );
        int propertyKeyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( key );
        statement.dataWriteOperations().nodeSetProperty( node, propertyKeyId, Values.of( value ) );
        return node;
    }

    private long constrainedNode( String labelName, String propertyKey, Object propertyValue )
            throws KernelException
    {
        long node;
        {
            Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );
            int label = statement.tokenWriteOperations().labelGetOrCreateForName( labelName );
            node = statement.dataWriteOperations().nodeCreate();
            statement.dataWriteOperations().nodeAddLabel( node, label );
            int key = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( propertyKey );
            statement.dataWriteOperations().nodeSetProperty( node, key, Values.of( propertyValue ) );
            commit();
        }
        createConstraint( labelName, propertyKey );
        return node;
    }

    private void createConstraint( String label, String propertyKey ) throws KernelException
    {
        int labelId;
        int propertyKeyId;
        TokenWriteOperations tokenWriteOperations = tokenWriteOperationsInNewTransaction();
        labelId = tokenWriteOperations.labelGetOrCreateForName( label );
        propertyKeyId = tokenWriteOperations.propertyKeyGetOrCreateForName( propertyKey );
        commit();

        SchemaWriteOperations schemaWriteOperations = schemaWriteOperationsInNewTransaction();
        schemaWriteOperations.uniquePropertyConstraintCreate( forLabel( labelId, propertyKeyId ) );
        commit();
    }
}

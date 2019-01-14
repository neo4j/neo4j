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
package org.neo4j.kernel.impl.api.integrationtest;

import org.junit.Test;

import org.neo4j.internal.kernel.api.CapableIndexReference;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.SilentTokenNameLookup;
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.values.storable.Values;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.internal.kernel.api.IndexQuery.exact;
import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.forLabel;

public class UniquenessConstraintValidationIT extends KernelIntegrationTest
{
    @Test
    public void shouldEnforceOnSetProperty() throws Exception
    {
        // given
        constrainedNode( "Label1", "key1", "value1" );

        Transaction transaction = newTransaction( AnonymousContext.writeToken() );

        // when
        long node = createLabeledNode( transaction, "Label1" );
        try
        {
            int propertyKeyId = transaction.tokenWrite().propertyKeyGetOrCreateForName( "key1" );
            transaction.dataWrite().nodeSetProperty( node, propertyKeyId, Values.of( "value1" ) );

            fail( "should have thrown exception" );
        }
        // then
        catch ( UniquePropertyValueValidationException e )
        {
            assertThat( e.getUserMessage( tokenLookup( transaction ) ), containsString( "`key1` = 'value1'" ) );
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

        Transaction transaction = newTransaction( AnonymousContext.writeToken() );

        long node = createLabeledNode( transaction, "label1" );

        assertNotEquals( firstNode, node );

        // When
        // a new node with the same constraint is added, with a value not equal but which would be mapped to the same double
        propertyValue++;
        // note how propertyValue is definitely not equal to propertyValue++ but they do equal if they are cast to double
        int propertyKeyId = transaction.tokenWrite().propertyKeyGetOrCreateForName( "key1" );
        transaction.dataWrite().nodeSetProperty( node, propertyKeyId, Values.of( propertyValue ) );

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
        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        long node = createNode( transaction, "key1", 1 );
        commit();

        transaction = newTransaction( AnonymousContext.writeToken() );
        try
        {
            int label = transaction.tokenWrite().labelGetOrCreateForName( "Label1" );
            transaction.dataWrite().nodeAddLabel( node, label );

            fail( "should have thrown exception" );
        }
        // then
        catch ( UniquePropertyValueValidationException e )
        {
            assertThat( e.getUserMessage( tokenLookup( transaction ) ), containsString( "`key1` = 1" ) );
        }
        commit();
    }

    @Test
    public void shouldEnforceUniquenessConstraintOnAddLabelForStringProperty() throws Exception
    {
        // given
        constrainedNode( "Label1", "key1", "value1" );

        Transaction transaction = newTransaction( AnonymousContext.writeToken() );

        // when
        long node = createNode( transaction, "key1", "value1" );
        try
        {
            int label = transaction.tokenWrite().labelGetOrCreateForName( "Label1" );
            transaction.dataWrite().nodeAddLabel( node, label );

            fail( "should have thrown exception" );
        }
        // then
        catch ( UniquePropertyValueValidationException e )
        {
            assertThat( e.getUserMessage( tokenLookup( transaction ) ), containsString( "`key1` = 'value1'" ) );
        }
        commit();
    }

    @Test
    public void shouldAllowRemoveAndAddConflictingDataInOneTransaction_DeleteNode() throws Exception
    {
        // given
        long node = constrainedNode( "Label1", "key1", "value1" );

        Transaction transaction = newTransaction( AnonymousContext.writeToken() );

        // when
        transaction.dataWrite().nodeDelete( node );
        createLabeledNode( transaction, "Label1", "key1", "value1" );
        commit();
    }

    @Test
    public void shouldAllowRemoveAndAddConflictingDataInOneTransaction_RemoveLabel() throws Exception
    {
        // given
        long node = constrainedNode( "Label1", "key1", "value1" );

        Transaction transaction = newTransaction( AnonymousContext.writeToken() );

        // when
        int label = transaction.tokenWrite().labelGetOrCreateForName( "Label1" );
        transaction.dataWrite().nodeRemoveLabel( node, label );
        createLabeledNode( transaction, "Label1", "key1", "value1" );
        commit();
    }

    @Test
    public void shouldAllowRemoveAndAddConflictingDataInOneTransaction_RemoveProperty() throws Exception
    {
        // given
        long node = constrainedNode( "Label1", "key1", "value1" );

        Transaction transaction = newTransaction( AnonymousContext.writeToken() );

        // when
        int key = transaction.tokenRead().propertyKey( "key1" );
        transaction.dataWrite().nodeRemoveProperty( node, key );
        createLabeledNode( transaction, "Label1", "key1", "value1" );
        commit();
    }

    @Test
    public void shouldAllowRemoveAndAddConflictingDataInOneTransaction_ChangeProperty() throws Exception
    {
        // given
        long node = constrainedNode( "Label1", "key1", "value1" );

        Transaction transaction = newTransaction( AnonymousContext.writeToken() );

        // when
        int propertyKeyId = transaction.tokenWrite().propertyKeyGetOrCreateForName( "key1" );
        transaction.dataWrite().nodeSetProperty( node, propertyKeyId, Values.of( "value2" ) );
        createLabeledNode( transaction, "Label1", "key1", "value1" );
        commit();
    }

    @Test
    public void shouldPreventConflictingDataInSameTransaction() throws Exception
    {
        // given
        constrainedNode( "Label1", "key1", "value1" );

        Transaction transaction = newTransaction( AnonymousContext.writeToken() );

        // when
        createLabeledNode( transaction, "Label1", "key1", "value2" );
        try
        {
            createLabeledNode( transaction, "Label1", "key1", "value2" );

            fail( "expected exception" );
        }
        // then
        catch ( UniquePropertyValueValidationException e )
        {
            assertThat( e.getUserMessage( tokenLookup( transaction ) ), containsString( "`key1` = 'value2'" ) );
        }
        commit();
    }

    @Test
    public void shouldAllowNoopPropertyUpdate() throws KernelException
    {
        // given
        long node = constrainedNode( "Label1", "key1", "value1" );

        Transaction transaction = newTransaction( AnonymousContext.writeToken() );

        // when
        int key = transaction.tokenWrite().propertyKeyGetOrCreateForName( "key1" );
        transaction.dataWrite().nodeSetProperty( node, key, Values.of( "value1" ) );

        // then should not throw exception
        commit();
    }

    @Test
    public void shouldAllowNoopLabelUpdate() throws KernelException
    {
        // given
        long node = constrainedNode( "Label1", "key1", "value1" );

        Transaction transaction = newTransaction( AnonymousContext.writeToken() );

        // when
        int label = transaction.tokenWrite().labelGetOrCreateForName( "Label1" );
        transaction.dataWrite().nodeAddLabel( node, label );

        // then should not throw exception
        commit();
    }

    @Test
    public void shouldAllowCreationOfNonConflictingData() throws Exception
    {
        // given
        constrainedNode( "Label1", "key1", "value1" );

        Transaction transaction = newTransaction( AnonymousContext.writeToken() );

        // when
        createNode( transaction, "key1", "value1" );
        createLabeledNode( transaction, "Label2", "key1", "value1" );
        createLabeledNode( transaction, "Label1", "key1", "value2" );
        createLabeledNode( transaction, "Label1", "key2", "value1" );

        commit();

        // then
        transaction = newTransaction( AnonymousContext.writeToken() );
        assertEquals( "number of nodes", 5, countNodes( transaction) );
        rollback();
    }

    @Test
    public void unrelatedNodesWithSamePropertyShouldNotInterfereWithUniquenessCheck() throws Exception
    {
        // given
        createConstraint( "Person", "id" );

        long ourNode;
        {
            Transaction transaction = newTransaction( AnonymousContext.writeToken() );
            ourNode = createLabeledNode( transaction, "Person", "id", 1 );
            createLabeledNode( transaction, "Item", "id", 2 );
            commit();
        }

        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        TokenRead tokenRead = transaction.tokenRead();
        int person = tokenRead.nodeLabel( "Person" );
        int propId = tokenRead.propertyKey( "id" );
        CapableIndexReference idx = transaction.schemaRead().index(  person, propId );

        // when
        createLabeledNode( transaction, "Item", "id", 2 );

        // then I should find the original node
        assertThat( transaction.dataRead().lockingNodeUniqueIndexSeek( idx, exact( propId, Values.of( 1 ) ) ),
                equalTo( ourNode ) );
        commit();
    }

    @Test
    public void addingUniqueNodeWithUnrelatedValueShouldNotAffectLookup() throws Exception
    {
        // given
        createConstraint( "Person", "id" );

        long ourNode;
        {
            Transaction transaction = newTransaction( AnonymousContext.writeToken() );
            ourNode = createLabeledNode( transaction, "Person", "id", 1 );
            commit();
        }

        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        TokenRead tokenRead = transaction.tokenRead();
        int person = tokenRead.nodeLabel( "Person" );
        int propId = tokenRead.propertyKey( "id" );
        CapableIndexReference idx = transaction.schemaRead().index( person, propId  );

        // when
        createLabeledNode( transaction, "Person", "id", 2 );

        // then I should find the original node
        assertThat( transaction.dataRead().lockingNodeUniqueIndexSeek( idx, exact( propId, Values.of( 1 ) ) ),
                equalTo( ourNode ) );
        commit();
    }

    private TokenNameLookup tokenLookup( Transaction transaction )
    {
        return new SilentTokenNameLookup( transaction.tokenRead() );
    }

    private long createLabeledNode( Transaction transaction, String label ) throws KernelException
    {
        long node = transaction.dataWrite().nodeCreate();
        int labelId = transaction.tokenWrite().labelGetOrCreateForName( label );
        transaction.dataWrite().nodeAddLabel( node, labelId );
        return node;
    }

    private long createNode( Transaction transaction, String key, Object value ) throws KernelException
    {
        long node = transaction.dataWrite().nodeCreate();
        int propertyKeyId = transaction.tokenWrite().propertyKeyGetOrCreateForName( key );
        transaction.dataWrite().nodeSetProperty( node, propertyKeyId, Values.of( value ) );
        return node;
    }

    private long createLabeledNode( Transaction transaction, String label, String key, Object value )
            throws KernelException
    {
        long node = createLabeledNode( transaction, label );
        int propertyKeyId = transaction.tokenWrite().propertyKeyGetOrCreateForName( key );
        transaction.dataWrite().nodeSetProperty( node, propertyKeyId, Values.of( value ) );
        return node;
    }

    private long constrainedNode( String labelName, String propertyKey, Object propertyValue )
            throws KernelException
    {
        long node;
        {
            Transaction transaction = newTransaction( AnonymousContext.writeToken() );
            int label = transaction.tokenWrite().labelGetOrCreateForName( labelName );
            node = transaction.dataWrite().nodeCreate();
            transaction.dataWrite().nodeAddLabel( node, label );
            int key = transaction.tokenWrite().propertyKeyGetOrCreateForName( propertyKey );
            transaction.dataWrite().nodeSetProperty( node, key, Values.of( propertyValue ) );
            commit();
        }
        createConstraint( labelName, propertyKey );
        return node;
    }

    private void createConstraint( String label, String propertyKey ) throws KernelException
    {
        int labelId;
        int propertyKeyId;
        TokenWrite tokenWrite = tokenWriteInNewTransaction();
        labelId = tokenWrite.labelGetOrCreateForName( label );
        propertyKeyId = tokenWrite.propertyKeyGetOrCreateForName( propertyKey );
        commit();

        SchemaWrite schemaWrite = schemaWriteInNewTransaction();
        schemaWrite.uniquePropertyConstraintCreate( forLabel( labelId, propertyKeyId ) );
        commit();
    }
}

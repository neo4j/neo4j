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

import java.util.Iterator;

import org.neo4j.internal.kernel.api.NamedToken;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.values.storable.Values;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.Iterators.asCollection;

public class PropertyIT extends KernelIntegrationTest
{
    @Test
    public void shouldBeAbleToSetAndReadLargeByteArray() throws Exception
    {
        // GIVEN
        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        long nodeId = transaction.dataWrite().nodeCreate();

        // WHEN
        int propertyKeyId = transaction.tokenWrite().propertyKeyGetOrCreateForName( "clown" );
        transaction.dataWrite().nodeSetProperty( nodeId, propertyKeyId, Values.of( new byte[100_000] ) );

        // WHEN
        commit();
        transaction = newTransaction();

        // THEN
        nodeGetProperty( transaction, nodeId, propertyKeyId );

        commit();
    }

    @Test
    public void nodeHasPropertyIfSet() throws Exception
    {
        // GIVEN
        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        long nodeId = transaction.dataWrite().nodeCreate();

        // WHEN
        int propertyKeyId = transaction.tokenWrite().propertyKeyGetOrCreateForName( "clown" );
        transaction.dataWrite().nodeSetProperty( nodeId, propertyKeyId, Values.stringValue( "bozo" ) );

        // THEN
        assertThat( nodeHasProperty( transaction, nodeId, propertyKeyId ), is( true ) );
        assertThat( nodeGetProperty( transaction, nodeId, propertyKeyId ),
                not( equalTo( Values.NO_VALUE ) ) );

        // WHEN
        commit();
        transaction = newTransaction();

        // THEN
        assertThat( nodeHasProperty( transaction, nodeId, propertyKeyId ), is( true ) );
        assertThat( nodeGetProperty( transaction, nodeId, propertyKeyId ), not( equalTo( Values.NO_VALUE ) ) );
        commit();
    }

    @Test
    public void nodeHasNotPropertyIfUnset() throws Exception
    {
        // GIVEN
        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        long nodeId = transaction.dataWrite().nodeCreate();

        // WHEN
        int propertyKeyId = transaction.tokenWrite().propertyKeyGetOrCreateForName( "clown" );

        // THEN
        assertThat( nodeHasProperty( transaction, nodeId, propertyKeyId ), is( false ) );
        assertThat( nodeGetProperty( transaction, nodeId, propertyKeyId ), equalTo( Values.NO_VALUE ) );

        // WHEN
        commit();

        transaction = newTransaction();

        // THEN
        assertThat( nodeHasProperty( transaction, nodeId, propertyKeyId ), is( false ) );
        assertThat( nodeGetProperty( transaction, nodeId, propertyKeyId ), equalTo( Values.NO_VALUE ) );
        commit();
    }

    @Test
    public void shouldRollbackSetNodePropertyValue() throws Exception
    {
        // GIVEN
        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        long nodeId = transaction.dataWrite().nodeCreate();
        int propertyKeyId = transaction.tokenWrite().propertyKeyGetOrCreateForName( "clown" );
        commit();

        // WHEN
        Write write = dataWriteInNewTransaction();
        write.nodeSetProperty( nodeId, propertyKeyId, Values.stringValue( "bozo" ) );
        rollback();

        // THEN
        transaction = newTransaction();
        assertThat( nodeHasProperty( transaction, nodeId, propertyKeyId ), is( false ) );
        assertThat( nodeGetProperty( transaction, nodeId, propertyKeyId ), equalTo( Values.NO_VALUE ) );
        commit();
    }

    @Test
    public void shouldUpdateNodePropertyValue() throws Exception
    {
        // GIVEN
        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        long nodeId = transaction.dataWrite().nodeCreate();
        int propertyId = transaction.tokenWrite().propertyKeyGetOrCreateForName( "clown" );
        transaction.dataWrite().nodeSetProperty( nodeId, propertyId, Values.stringValue( "bozo" ) );
        commit();

        // WHEN
        Write write = dataWriteInNewTransaction();
        write.nodeSetProperty( nodeId, propertyId, Values.of( 42 ) );
        commit();

        // THEN
        transaction = newTransaction();
        assertEquals( 42, nodeGetProperty( transaction, nodeId, propertyId ).asObject() );
        commit();
    }

    @Test
    public void shouldListAllPropertyKeys() throws Exception
    {
        // given
        dbWithNoCache();

        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        int prop1 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "prop1" );
        int prop2 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "prop2" );

        // when
        Iterator<NamedToken> propIdsBeforeCommit = transaction.tokenRead().propertyKeyGetAllTokens();

        // then
        assertThat( asCollection( propIdsBeforeCommit ),
                hasItems( new NamedToken( "prop1", prop1 ), new NamedToken( "prop2", prop2 ) ) );

        // when
        commit();
        transaction = newTransaction();
        Iterator<NamedToken> propIdsAfterCommit = transaction.tokenRead().propertyKeyGetAllTokens();

        // then
        assertThat( asCollection( propIdsAfterCommit ),
                hasItems( new NamedToken( "prop1", prop1 ), new NamedToken( "prop2", prop2 ) ) );
        commit();
    }

    @Test
    public void shouldNotAllowModifyingPropertiesOnDeletedNode() throws Exception
    {
        // given
        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        int prop1 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "prop1" );
        long node = transaction.dataWrite().nodeCreate();

        transaction.dataWrite().nodeSetProperty( node, prop1, Values.stringValue( "As" ) );
        transaction.dataWrite().nodeDelete( node );

        // When
        try
        {
            transaction.dataWrite().nodeRemoveProperty( node, prop1 );
            fail( "Should have failed." );
        }
        catch ( EntityNotFoundException e )
        {
            assertThat( e.getMessage(), equalTo( "Unable to load NODE with id " + node + "." ) );
        }
        commit();
    }

    @Test
    public void shouldNotAllowModifyingPropertiesOnDeletedRelationship() throws Exception
    {
        // given
        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        int prop1 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "prop1" );
        int type = transaction.tokenWrite().relationshipTypeGetOrCreateForName( "RELATED" );
        long startNodeId = transaction.dataWrite().nodeCreate();
        long endNodeId = transaction.dataWrite().nodeCreate();
        long rel = transaction.dataWrite().relationshipCreate( startNodeId, type, endNodeId );

        transaction.dataWrite().relationshipSetProperty( rel, prop1, Values.stringValue( "As" ) );
        transaction.dataWrite().relationshipDelete( rel );

        // When
        try
        {
            transaction.dataWrite().relationshipRemoveProperty( rel, prop1 );
            fail( "Should have failed." );
        }
        catch ( EntityNotFoundException e )
        {
            assertThat( e.getMessage(), equalTo( "Unable to load RELATIONSHIP with id " + rel + "." ) );
        }
        commit();
    }

    @Test
    public void shouldBeAbleToRemoveResetAndTwiceRemovePropertyOnNode() throws Exception
    {
        // given
        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        int prop = transaction.tokenWrite().propertyKeyGetOrCreateForName( "foo" );

        long node = transaction.dataWrite().nodeCreate();
        transaction.dataWrite().nodeSetProperty( node, prop, Values.of( "bar" ) );

        commit();

        // when
        Write write = dataWriteInNewTransaction();
        write.nodeRemoveProperty( node, prop );
        write.nodeSetProperty( node, prop, Values.of( "bar" ) );
        write.nodeRemoveProperty( node, prop );
        write.nodeRemoveProperty( node, prop );

        commit();

        // then
        transaction = newTransaction();
        assertThat( nodeGetProperty( transaction, node, prop ), equalTo( Values.NO_VALUE ) );
        commit();
    }

    @Test
    public void shouldBeAbleToRemoveResetAndTwiceRemovePropertyOnRelationship() throws Exception
    {
        // given
        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        int prop = transaction.tokenWrite().propertyKeyGetOrCreateForName( "foo" );
        int type = transaction.tokenWrite().relationshipTypeGetOrCreateForName( "RELATED" );

        long startNodeId = transaction.dataWrite().nodeCreate();
        long endNodeId = transaction.dataWrite().nodeCreate();
        long rel = transaction.dataWrite().relationshipCreate( startNodeId, type, endNodeId );
        transaction.dataWrite().relationshipSetProperty( rel, prop, Values.of( "bar" ) );

        commit();

        // when
        Write write = dataWriteInNewTransaction();
        write.relationshipRemoveProperty( rel, prop );
        write.relationshipSetProperty( rel, prop, Values.of( "bar" ) );
        write.relationshipRemoveProperty( rel, prop );
        write.relationshipRemoveProperty( rel, prop );

        commit();

        // then
        transaction = newTransaction();
        assertThat( relationshipGetProperty(transaction, rel, prop ), equalTo( Values.NO_VALUE ) );
        commit();
    }
}


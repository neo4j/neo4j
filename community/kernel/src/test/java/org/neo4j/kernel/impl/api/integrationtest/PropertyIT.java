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

import java.util.Collections;
import java.util.Iterator;

import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.storageengine.api.Token;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.collection.primitive.PrimitiveIntCollections.toList;
import static org.neo4j.helpers.collection.Iterators.asCollection;
import static org.neo4j.values.storable.Values.NO_VALUE;

public class PropertyIT extends KernelIntegrationTest
{
    @Test
    public void shouldBeAbleToSetAndReadLargeByteArray() throws Exception
    {
        // GIVEN
        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );
        long nodeId = statement.dataWriteOperations().nodeCreate();

        // WHEN
        int propertyKeyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( "clown" );
        statement.dataWriteOperations().nodeSetProperty( nodeId, propertyKeyId, Values.of( new byte[100_000] ) );

        // WHEN
        commit();
        ReadOperations readOperations = readOperationsInNewTransaction();

        // THEN
        readOperations.nodeGetProperty( nodeId, propertyKeyId );

        commit();
    }

    @Test
    public void shouldSetNodePropertyValue() throws Exception
    {
        // GIVEN
        Value value = Values.of( "bozo" );
        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );

        long nodeId = statement.dataWriteOperations().nodeCreate();

        // WHEN
        int propertyKeyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( "clown" );
        statement.dataWriteOperations().nodeSetProperty( nodeId, propertyKeyId, value );

        // THEN
        assertEquals( value, statement.readOperations().nodeGetProperty( nodeId, propertyKeyId ) );

        // WHEN
        commit();
        ReadOperations readOperations = readOperationsInNewTransaction();

        // THEN
        assertEquals( value, readOperations.nodeGetProperty( nodeId, propertyKeyId ) );
        commit();
    }

    @Test
    public void shouldRemoveSetNodeProperty() throws Exception
    {
        // GIVEN
        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );

        long nodeId = statement.dataWriteOperations().nodeCreate();
        int propertyKeyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( "clown" );
        statement.dataWriteOperations().nodeSetProperty( nodeId, propertyKeyId, Values.stringValue( "bozo" ) );

        // WHEN
        statement.dataWriteOperations().nodeRemoveProperty( nodeId, propertyKeyId );

        // THEN
        assertThat( statement.readOperations().nodeGetProperty( nodeId, propertyKeyId ), equalTo( Values.NO_VALUE ) );

        // WHEN
        commit();

        // THEN
        ReadOperations readOperations = readOperationsInNewTransaction();
        assertThat( readOperations.nodeGetProperty( nodeId, propertyKeyId ), equalTo( Values.NO_VALUE ) );
        commit();
    }

    @Test
    public void shouldRemoveSetNodePropertyAcrossTransactions() throws Exception
    {
        // GIVEN
        int propertyKeyId;
        long nodeId;
        {
            Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );
            nodeId = statement.dataWriteOperations().nodeCreate();
            propertyKeyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( "clown" );
            statement.dataWriteOperations().nodeSetProperty( nodeId, propertyKeyId, Values.stringValue( "bozo" ) );
            commit();
        }
        {
            Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );

            // WHEN
            Value previous = statement.dataWriteOperations().nodeRemoveProperty( nodeId, propertyKeyId );

            // THEN
            assertTrue( previous.equals( "bozo" ) );
            assertThat( statement.readOperations().nodeGetProperty( nodeId, propertyKeyId ), equalTo( Values.NO_VALUE ) );

            // WHEN
            commit();
        }

        // THEN
        ReadOperations readOperations = readOperationsInNewTransaction();
        assertThat( readOperations.nodeGetProperty( nodeId, propertyKeyId ), equalTo( Values.NO_VALUE ) );
        commit();
    }

    @Test
    public void shouldRemoveSetExistingProperty() throws Exception
    {
        // GIVEN
        dbWithNoCache();

        int propertyKeyId;
        long nodeId;
        {
            Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );
            nodeId = statement.dataWriteOperations().nodeCreate();
            propertyKeyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( "clown" );
            statement.dataWriteOperations().nodeSetProperty( nodeId, propertyKeyId, Values.stringValue( "bozo" ) );
            commit();
        }

        Value newValue = Values.stringValue( "ozob" );

        {
            Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );

            // WHEN
            statement.dataWriteOperations().nodeRemoveProperty( nodeId, propertyKeyId );
            statement.dataWriteOperations().nodeSetProperty( nodeId, propertyKeyId, newValue );

            // THEN
            assertThat( statement.readOperations().nodeGetProperty( nodeId, propertyKeyId ), equalTo( newValue ) );

            // WHEN
            commit();
        }

        // THEN
        {
            ReadOperations readOperations = readOperationsInNewTransaction();
            assertThat( readOperations.nodeGetProperty( nodeId, propertyKeyId ), equalTo( newValue ) );
            assertThat( toList( readOperations.nodeGetPropertyKeys( nodeId ) ),
                    equalTo( Collections.singletonList( propertyKeyId ) ) );
            commit();
        }
    }

    @Test
    public void shouldSilentlyNotRemoveMissingNodeProperty() throws Exception
    {
        // GIVEN
        int propertyId;
        long nodeId;
        {
            Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );
            nodeId = statement.dataWriteOperations().nodeCreate();
            propertyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( "clown" );
            commit();
        }
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();

            // WHEN
            Value result = statement.nodeRemoveProperty( nodeId, propertyId );

            // THEN
            assertTrue( "Return no property if removing missing", result == NO_VALUE );
            commit();
        }
    }

    @Test
    public void nodeHasPropertyIfSet() throws Exception
    {
        // GIVEN
        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );
        long nodeId = statement.dataWriteOperations().nodeCreate();

        // WHEN
        int propertyKeyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( "clown" );
        statement.dataWriteOperations().nodeSetProperty( nodeId, propertyKeyId, Values.stringValue( "bozo" ) );

        // THEN
        assertThat( statement.readOperations().nodeHasProperty( nodeId, propertyKeyId ), is( true ) );
        assertThat( statement.readOperations().nodeGetProperty( nodeId, propertyKeyId ),
                not( equalTo( Values.NO_VALUE ) ) );

        // WHEN
        commit();
        ReadOperations readOperations = readOperationsInNewTransaction();

        // THEN
        assertThat( readOperations.nodeHasProperty( nodeId, propertyKeyId ), is( true ) );
        assertThat( readOperations.nodeGetProperty( nodeId, propertyKeyId ), not( equalTo( Values.NO_VALUE ) ) );
        commit();
    }

    @Test
    public void nodeHasNotPropertyIfUnset() throws Exception
    {
        // GIVEN
        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );
        long nodeId = statement.dataWriteOperations().nodeCreate();

        // WHEN
        int propertyKeyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( "clown" );

        // THEN
        assertThat( statement.readOperations().nodeHasProperty( nodeId, propertyKeyId ), is( false ) );
        assertThat( statement.readOperations().nodeGetProperty( nodeId, propertyKeyId ), equalTo( Values.NO_VALUE ) );

        // WHEN
        commit();

        ReadOperations readOperations = readOperationsInNewTransaction();

        // THEN
        assertThat( readOperations.nodeHasProperty( nodeId, propertyKeyId ), is( false ) );
        assertThat( readOperations.nodeGetProperty( nodeId, propertyKeyId ), equalTo( Values.NO_VALUE ) );
        commit();
    }

    @Test
    public void shouldRollbackSetNodePropertyValue() throws Exception
    {
        // GIVEN
        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );
        long nodeId = statement.dataWriteOperations().nodeCreate();
        int propertyKeyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( "clown" );
        commit();

        // WHEN
        DataWriteOperations dataWriteOperations = dataWriteOperationsInNewTransaction();
        dataWriteOperations.nodeSetProperty( nodeId, propertyKeyId, Values.stringValue( "bozo" ) );
        rollback();

        // THEN
        ReadOperations readOperations = readOperationsInNewTransaction();
        assertThat( readOperations.nodeHasProperty( nodeId, propertyKeyId ), is( false ) );
        assertThat( readOperations.nodeGetProperty( nodeId, propertyKeyId ), equalTo( Values.NO_VALUE ) );
        commit();
    }

    @Test
    public void shouldUpdateNodePropertyValue() throws Exception
    {
        // GIVEN
        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );
        long nodeId = statement.dataWriteOperations().nodeCreate();
        int propertyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( "clown" );
        statement.dataWriteOperations().nodeSetProperty( nodeId, propertyId, Values.stringValue( "bozo" ) );
        commit();

        // WHEN
        DataWriteOperations dataWriteOperations = dataWriteOperationsInNewTransaction();
        dataWriteOperations.nodeSetProperty( nodeId, propertyId, Values.of( 42 ) );
        commit();

        // THEN
        ReadOperations readOperations = readOperationsInNewTransaction();
        assertEquals( 42, readOperations.nodeGetProperty( nodeId, propertyId ).asObject() );
        commit();
    }

    @Test
    public void shouldListAllPropertyKeys() throws Exception
    {
        // given
        dbWithNoCache();

        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );
        int prop1 = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( "prop1" );
        int prop2 = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( "prop2" );

        // when
        Iterator<Token> propIdsBeforeCommit = statement.readOperations().propertyKeyGetAllTokens();

        // then
        assertThat( asCollection( propIdsBeforeCommit ),
                hasItems( new Token( "prop1", prop1 ), new Token( "prop2", prop2 ) ) );

        // when
        commit();
        ReadOperations readOperations = readOperationsInNewTransaction();
        Iterator<Token> propIdsAfterCommit = readOperations.propertyKeyGetAllTokens();

        // then
        assertThat( asCollection( propIdsAfterCommit ),
                hasItems( new Token( "prop1", prop1 ), new Token( "prop2", prop2 ) ) );
        commit();
    }

    @Test
    public void shouldNotAllowModifyingPropertiesOnDeletedNode() throws Exception
    {
        // given
        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );
        int prop1 = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( "prop1" );
        long node = statement.dataWriteOperations().nodeCreate();

        statement.dataWriteOperations().nodeSetProperty( node, prop1, Values.stringValue( "As" ) );
        statement.dataWriteOperations().nodeDelete( node );

        // When
        try
        {
            statement.dataWriteOperations().nodeRemoveProperty( node, prop1 );
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
        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );
        int prop1 = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( "prop1" );
        int type = statement.tokenWriteOperations().relationshipTypeGetOrCreateForName( "RELATED" );
        long startNodeId = statement.dataWriteOperations().nodeCreate();
        long endNodeId = statement.dataWriteOperations().nodeCreate();
        long rel = statement.dataWriteOperations().relationshipCreate( type, startNodeId, endNodeId );

        statement.dataWriteOperations().relationshipSetProperty( rel, prop1, Values.stringValue( "As" ) );
        statement.dataWriteOperations().relationshipDelete( rel );

        // When
        try
        {
            statement.dataWriteOperations().relationshipRemoveProperty( rel, prop1 );
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
        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );
        int prop = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( "foo" );

        long node = statement.dataWriteOperations().nodeCreate();
        statement.dataWriteOperations().nodeSetProperty( node, prop, Values.of( "bar" ) );

        commit();

        // when
        DataWriteOperations dataWriteOperations = dataWriteOperationsInNewTransaction();
        dataWriteOperations.nodeRemoveProperty( node, prop );
        dataWriteOperations.nodeSetProperty( node, prop, Values.of( "bar" ) );
        dataWriteOperations.nodeRemoveProperty( node, prop );
        dataWriteOperations.nodeRemoveProperty( node, prop );

        commit();

        // then
        ReadOperations readOperations = readOperationsInNewTransaction();
        assertThat( readOperations.nodeGetProperty( node, prop ), equalTo( Values.NO_VALUE ) );
        commit();
    }

    @Test
    public void shouldBeAbleToRemoveResetAndTwiceRemovePropertyOnRelationship() throws Exception
    {
        // given
        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );
        int prop = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( "foo" );
        int type = statement.tokenWriteOperations().relationshipTypeGetOrCreateForName( "RELATED" );

        long startNodeId = statement.dataWriteOperations().nodeCreate();
        long endNodeId = statement.dataWriteOperations().nodeCreate();
        long rel = statement.dataWriteOperations().relationshipCreate( type, startNodeId, endNodeId );
        statement.dataWriteOperations().relationshipSetProperty( rel, prop, Values.of( "bar" ) );

        commit();

        // when
        DataWriteOperations dataWriteOperations = dataWriteOperationsInNewTransaction();
        dataWriteOperations.relationshipRemoveProperty( rel, prop );
        dataWriteOperations.relationshipSetProperty( rel, prop, Values.of( "bar" ) );
        dataWriteOperations.relationshipRemoveProperty( rel, prop );
        dataWriteOperations.relationshipRemoveProperty( rel, prop );

        commit();

        // then
        ReadOperations readOperations = readOperationsInNewTransaction();
        assertThat( readOperations.relationshipGetProperty( rel, prop ), equalTo( Values.NO_VALUE ) );
        commit();
    }
}


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

import java.util.Iterator;

import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.storageengine.api.Token;

import static java.util.Arrays.asList;
import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.neo4j.collection.primitive.PrimitiveIntCollections.toList;
import static org.neo4j.helpers.collection.Iterators.asCollection;
import static org.neo4j.kernel.api.properties.Property.byteArrayProperty;
import static org.neo4j.kernel.api.properties.Property.property;
import static org.neo4j.kernel.api.properties.Property.stringProperty;

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
        DefinedProperty property = byteArrayProperty( propertyKeyId, new byte[100_000] );
        statement.dataWriteOperations().nodeSetProperty( nodeId, property );

        // WHEN
        commit();
        ReadOperations readOperations = readOperationsInNewTransaction();

        // THEN
        readOperations.nodeGetProperty( nodeId, propertyKeyId );
    }

    @Test
    public void shouldSetNodePropertyValue() throws Exception
    {
        // GIVEN
        String value = "bozo";
        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );

        long nodeId = statement.dataWriteOperations().nodeCreate();

        // WHEN
        int propertyKeyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( "clown" );
        statement.dataWriteOperations().nodeSetProperty( nodeId, stringProperty( propertyKeyId, value ) );

        // THEN
        assertEquals( value, statement.readOperations().nodeGetProperty( nodeId, propertyKeyId ) );

        // WHEN
        commit();
        ReadOperations readOperations = readOperationsInNewTransaction();

        // THEN
        assertEquals( value, readOperations.nodeGetProperty( nodeId, propertyKeyId ) );
    }

    @Test
    public void shouldRemoveSetNodeProperty() throws Exception
    {
        // GIVEN
        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );

        long nodeId = statement.dataWriteOperations().nodeCreate();
        int propertyKeyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( "clown" );
        statement.dataWriteOperations().nodeSetProperty( nodeId, stringProperty( propertyKeyId, "bozo" ) );

        // WHEN
        statement.dataWriteOperations().nodeRemoveProperty( nodeId, propertyKeyId );

        // THEN
        assertThat( statement.readOperations().nodeGetProperty( nodeId, propertyKeyId ), nullValue() );

        // WHEN
        commit();

        // THEN
        ReadOperations readOperations = readOperationsInNewTransaction();
        assertThat( readOperations.nodeGetProperty( nodeId, propertyKeyId ), nullValue() );
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
            statement.dataWriteOperations().nodeSetProperty( nodeId, stringProperty( propertyKeyId, "bozo" ) );
            commit();
        }
        {
            Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );

            // WHEN
            Object previous = statement.dataWriteOperations().nodeRemoveProperty( nodeId, propertyKeyId ).value();

            // THEN
            assertEquals( "bozo", previous );
            assertThat( statement.readOperations().nodeGetProperty( nodeId, propertyKeyId ), nullValue() );

            // WHEN
            commit();
        }

        // THEN
        ReadOperations readOperations = readOperationsInNewTransaction();
        assertThat( readOperations.nodeGetProperty( nodeId, propertyKeyId ), nullValue() );
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
            statement.dataWriteOperations().nodeSetProperty( nodeId, stringProperty( propertyKeyId, "bozo" ) );
            commit();
        }

        DefinedProperty newProperty = stringProperty( propertyKeyId, "ozob" );

        {
            Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );

            // WHEN
            statement.dataWriteOperations().nodeRemoveProperty( nodeId, propertyKeyId );
            statement.dataWriteOperations().nodeSetProperty( nodeId, newProperty );

            // THEN
            assertThat( statement.readOperations().nodeGetProperty( nodeId, propertyKeyId ),
                    equalTo( newProperty.value() ) );

            // WHEN
            commit();
        }

        // THEN
        {
            ReadOperations readOperations = readOperationsInNewTransaction();
            assertThat( readOperations.nodeGetProperty( nodeId, propertyKeyId ), equalTo( newProperty.value() ) );
            assertThat( toList( readOperations.nodeGetPropertyKeys( nodeId ) ),
                    equalTo( asList( newProperty.propertyKeyId() ) ) );
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
            Property result = statement.nodeRemoveProperty( nodeId, propertyId );

            // THEN
            assertFalse( "Return no property if removing missing", result.isDefined() );
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
        statement.dataWriteOperations().nodeSetProperty( nodeId, stringProperty( propertyKeyId, "bozo" ) );

        // THEN
        assertThat( statement.readOperations().nodeHasProperty( nodeId, propertyKeyId ), is( true ) );
        assertThat( statement.readOperations().nodeGetProperty( nodeId, propertyKeyId ), notNullValue() );

        // WHEN
        commit();
        ReadOperations readOperations = readOperationsInNewTransaction();

        // THEN
        assertThat( readOperations.nodeHasProperty( nodeId, propertyKeyId ), is( true ) );
        assertThat( readOperations.nodeGetProperty( nodeId, propertyKeyId ), notNullValue() );
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
        assertThat( statement.readOperations().nodeGetProperty( nodeId, propertyKeyId ), nullValue() );

        // WHEN
        commit();

        ReadOperations readOperations = readOperationsInNewTransaction();

        // THEN
        assertThat( readOperations.nodeHasProperty( nodeId, propertyKeyId ), is( false ) );
        assertThat( readOperations.nodeGetProperty( nodeId, propertyKeyId ), nullValue() );
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
        dataWriteOperations.nodeSetProperty( nodeId, stringProperty( propertyKeyId, "bozo" ) );
        rollback();

        // THEN
        ReadOperations readOperations = readOperationsInNewTransaction();
        assertThat( readOperations.nodeHasProperty( nodeId, propertyKeyId ), is( false ) );
        assertThat( readOperations.nodeGetProperty( nodeId, propertyKeyId ), nullValue() );
    }

    @Test
    public void shouldUpdateNodePropertyValue() throws Exception
    {
        // GIVEN
        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );
        long nodeId = statement.dataWriteOperations().nodeCreate();
        int propertyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( "clown" );
        statement.dataWriteOperations().nodeSetProperty( nodeId, stringProperty( propertyId, "bozo" ) );
        commit();

        // WHEN
        DataWriteOperations dataWriteOperations = dataWriteOperationsInNewTransaction();
        dataWriteOperations.nodeSetProperty( nodeId, Property.intProperty( propertyId, 42 ) );
        commit();

        // THEN
        ReadOperations readOperations = readOperationsInNewTransaction();
        assertEquals( 42, readOperations.nodeGetProperty( nodeId, propertyId ) );
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
    }

    @Test
    public void shouldNotAllowModifyingPropertiesOnDeletedNode() throws Exception
    {
        // given
        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );
        int prop1 = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( "prop1" );
        long node = statement.dataWriteOperations().nodeCreate();

        statement.dataWriteOperations().nodeSetProperty( node, stringProperty( prop1, "As" ) );
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

        statement.dataWriteOperations().relationshipSetProperty( rel, stringProperty( prop1, "As" ) );
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
    }

    @Test
    public void shouldBeAbleToRemoveResetAndTwiceRemovePropertyOnNode() throws Exception
    {
        // given
        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );
        int prop = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( "foo" );

        long node = statement.dataWriteOperations().nodeCreate();
        statement.dataWriteOperations().nodeSetProperty( node, property( prop, "bar" ) );

        commit();

        // when
        DataWriteOperations dataWriteOperations = dataWriteOperationsInNewTransaction();
        dataWriteOperations.nodeRemoveProperty( node, prop );
        dataWriteOperations.nodeSetProperty( node, property( prop, "bar" ) );
        dataWriteOperations.nodeRemoveProperty( node, prop );
        dataWriteOperations.nodeRemoveProperty( node, prop );

        commit();

        // then
        ReadOperations readOperations = readOperationsInNewTransaction();
        assertThat( readOperations.nodeGetProperty( node, prop ), nullValue() );
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
        statement.dataWriteOperations().relationshipSetProperty( rel, property( prop, "bar" ) );

        commit();

        // when
        DataWriteOperations dataWriteOperations = dataWriteOperationsInNewTransaction();
        dataWriteOperations.relationshipRemoveProperty( rel, prop );
        dataWriteOperations.relationshipSetProperty( rel, property( prop, "bar" ) );
        dataWriteOperations.relationshipRemoveProperty( rel, prop );
        dataWriteOperations.relationshipRemoveProperty( rel, prop );

        commit();

        // then
        ReadOperations readOperations = readOperationsInNewTransaction();
        assertThat( readOperations.relationshipGetProperty( rel, prop ), nullValue() );
    }
}


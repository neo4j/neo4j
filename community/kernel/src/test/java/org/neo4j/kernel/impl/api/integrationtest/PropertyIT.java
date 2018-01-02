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

import java.util.Arrays;
import java.util.Iterator;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.core.Token;

import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.kernel.api.properties.Property.byteArrayProperty;
import static org.neo4j.kernel.api.properties.Property.property;
import static org.neo4j.kernel.api.properties.Property.stringProperty;

public class PropertyIT extends KernelIntegrationTest
{
    @Test
    public void shouldBeAbleToSetAndReadLargeByteArray() throws Exception
    {
        // GIVEN
        int propertyKeyId;
        long nodeId;
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();
            nodeId = statement.nodeCreate();

            // WHEN
            propertyKeyId = statement.propertyKeyGetOrCreateForName( "clown" );
            statement.nodeSetProperty( nodeId, byteArrayProperty( propertyKeyId, new byte[100_000] ) );

            // WHEN
            commit();
        }
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();

            // THEN
            Object value = statement.nodeGetProperty( nodeId, propertyKeyId );
        }
    }

    @Test
    public void shouldSetNodePropertyValue() throws Exception
    {
        // GIVEN
        int propertyKeyId;
        String value = "bozo";
        long nodeId;
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();
            nodeId = statement.nodeCreate();

            // WHEN
            propertyKeyId = statement.propertyKeyGetOrCreateForName( "clown" );
            statement.nodeSetProperty( nodeId, stringProperty( propertyKeyId, value ) );

            // THEN
            assertEquals( value, statement.nodeGetProperty(
                    nodeId, propertyKeyId ) );

            // WHEN
            commit();
        }
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();

            // THEN
            assertEquals( value, statement.nodeGetProperty( nodeId, propertyKeyId ) );
        }
    }

    @Test
    public void shouldRemoveSetNodeProperty() throws Exception
    {
        // GIVEN
        int propertyKeyId;
        long nodeId;
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();
            nodeId = statement.nodeCreate();
            propertyKeyId = statement.propertyKeyGetOrCreateForName( "clown" );
            statement.nodeSetProperty( nodeId, stringProperty( propertyKeyId, "bozo" ) );

            // WHEN
            statement.nodeRemoveProperty( nodeId, propertyKeyId );

            // THEN
            assertThat( statement.nodeGetProperty( nodeId, propertyKeyId ), nullValue() );

            // WHEN
            commit();
        }

        // THEN
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();
            assertThat( statement.nodeGetProperty( nodeId, propertyKeyId ), nullValue() );
        }
    }

    @Test
    public void shouldRemoveSetNodePropertyAcrossTransactions() throws Exception
    {
        // GIVEN
        int propertyKeyId;
        long nodeId;
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();
            nodeId = statement.nodeCreate();
            propertyKeyId = statement.propertyKeyGetOrCreateForName( "clown" );
            statement.nodeSetProperty( nodeId, stringProperty( propertyKeyId, "bozo" ) );
            commit();
        }
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();

            // WHEN
            Object previous = statement.nodeRemoveProperty( nodeId, propertyKeyId ).value();

            // THEN
            assertEquals( "bozo", previous );
            assertThat( statement.nodeGetProperty( nodeId, propertyKeyId ), nullValue() );

            // WHEN
            commit();
        }

        // THEN
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();
            assertThat( statement.nodeGetProperty( nodeId, propertyKeyId ), nullValue() );
        }
    }

    @Test
    public void shouldRemoveSetExistingProperty() throws Exception
    {
        // GIVEN
        dbWithNoCache();

        int propertyKeyId;
        long nodeId;
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();
            nodeId = statement.nodeCreate();
            propertyKeyId = statement.propertyKeyGetOrCreateForName( "clown" );
            statement.nodeSetProperty( nodeId, stringProperty( propertyKeyId, "bozo" ) );
            commit();
        }

        DefinedProperty newProperty = stringProperty( propertyKeyId, "ozob" );

        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();

            // WHEN
            statement.nodeRemoveProperty( nodeId, propertyKeyId );
            statement.nodeSetProperty( nodeId, newProperty );

            // THEN
            assertThat( statement.nodeGetProperty( nodeId, propertyKeyId ), equalTo( newProperty.value() ) );

            // WHEN
            commit();
        }

        // THEN
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();
            assertThat( statement.nodeGetProperty( nodeId, propertyKeyId ), equalTo( newProperty.value() ) );
            assertThat( IteratorUtil.asList( statement.nodeGetPropertyKeys( nodeId ) ), equalTo( Arrays.asList(
                    newProperty.propertyKeyId() ) ) );
        }
    }

    @Test
    public void shouldSilentlyNotRemoveMissingNodeProperty() throws Exception
    {
        // GIVEN
        int propertyId;
        long nodeId;
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();
            nodeId = statement.nodeCreate();
            propertyId = statement.propertyKeyGetOrCreateForName( "clown" );
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
        int propertyKeyId;
        long nodeId;
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();
            nodeId = statement.nodeCreate();

            // WHEN
            propertyKeyId = statement.propertyKeyGetOrCreateForName( "clown" );
            statement.nodeSetProperty( nodeId, stringProperty( propertyKeyId, "bozo" ) );

            // THEN
            assertThat( statement.nodeHasProperty( nodeId, propertyKeyId ), is( true ) );
            assertThat( statement.nodeGetProperty( nodeId, propertyKeyId ), notNullValue() );

            // WHEN
            commit();
        }
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();

            // THEN
            assertThat( statement.nodeHasProperty( nodeId, propertyKeyId ), is( true ) );
            assertThat( statement.nodeGetProperty( nodeId, propertyKeyId ), notNullValue() );
        }
    }

    @Test
    public void nodeHasNotPropertyIfUnset() throws Exception
    {
        int propertyKeyId;
        long nodeId;
        {
            // GIVEN
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();
            nodeId = statement.nodeCreate();

            // WHEN
            propertyKeyId = statement.propertyKeyGetOrCreateForName( "clown" );

            // THEN
            assertThat( statement.nodeHasProperty( nodeId, propertyKeyId ), is( false ) );
            assertThat( statement.nodeGetProperty( nodeId, propertyKeyId ), nullValue() );

            // WHEN
            commit();
        }

        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();

            // THEN
            assertThat( statement.nodeHasProperty( nodeId, propertyKeyId ), is( false ) );
            assertThat( statement.nodeGetProperty( nodeId, propertyKeyId ), nullValue() );
        }
    }

    @Test
    public void shouldRollbackSetNodePropertyValue() throws Exception
    {
        // GIVEN
        int propertyKeyId;
        long nodeId;
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();
            nodeId = statement.nodeCreate();
            propertyKeyId = statement.propertyKeyGetOrCreateForName( "clown" );
            commit();
        }

        // WHEN
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();
            statement.nodeSetProperty( nodeId, stringProperty( propertyKeyId, "bozo" ) );
            rollback();
        }

        // THEN
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();
            assertThat( statement.nodeHasProperty( nodeId, propertyKeyId ), is( false ) );
            assertThat( statement.nodeGetProperty( nodeId, propertyKeyId ), nullValue() );
        }
    }

    @Test
    public void shouldUpdateNodePropertyValue() throws Exception
    {
        // GIVEN
        int propertyId;
        long nodeId;
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();
            nodeId = statement.nodeCreate();
            propertyId = statement.propertyKeyGetOrCreateForName( "clown" );
            statement.nodeSetProperty( nodeId, stringProperty( propertyId, "bozo" ) );
            commit();
        }

        // WHEN
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();
            statement.nodeSetProperty( nodeId, Property.intProperty( propertyId, 42 ) );
            commit();
        }

        // THEN
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();
            assertEquals( 42, statement.nodeGetProperty( nodeId, propertyId ) );
        }
    }

    @Test
    public void shouldListAllPropertyKeys() throws Exception
    {
        // given
        dbWithNoCache();

        long prop1;
        long prop2;
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();
            prop1 = statement.propertyKeyGetOrCreateForName( "prop1" );
            prop2 = statement.propertyKeyGetOrCreateForName( "prop2" );

            // when
            Iterator<Token> propIdsBeforeCommit = statement.propertyKeyGetAllTokens();

            // then
            assertThat( asCollection( propIdsBeforeCommit ),
                    hasItems( new Token( "prop1", (int) prop1 ), new Token( "prop2", (int) prop2 ) ) );

            // when
            commit();
        }
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();
            Iterator<Token> propIdsAfterCommit = statement.propertyKeyGetAllTokens();

            // then
            assertThat( asCollection( propIdsAfterCommit ),
                    hasItems( new Token( "prop1", (int) prop1 ), new Token( "prop2", (int) prop2 ) ) );
        }
    }

    @Test
    public void shouldNotAllowModifyingPropertiesOnDeletedNode() throws Exception
    {
        // given
        int prop1;
        long node;
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();
            prop1 = statement.propertyKeyGetOrCreateForName( "prop1" );
            node = statement.nodeCreate();

            statement.nodeSetProperty( node, stringProperty( prop1, "As" ) );
            statement.nodeDelete( node );

            // When
            try
            {
                statement.nodeRemoveProperty( node, prop1 );
                fail( "Should have failed." );
            }
            catch ( EntityNotFoundException e )
            {
                assertThat( e.getMessage(),
                            equalTo( "Unable to load NODE with id " + node + "." ) );
            }
        }
    }

    @Test
    public void shouldNotAllowModifyingPropertiesOnDeletedRelationship() throws Exception
    {
        // given
        int prop1;
        long rel;
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();
            prop1 = statement.propertyKeyGetOrCreateForName( "prop1" );
            int type = statement.relationshipTypeGetOrCreateForName( "RELATED" );
            rel = statement.relationshipCreate( type, statement.nodeCreate(), statement.nodeCreate() );

            statement.relationshipSetProperty( rel, stringProperty( prop1, "As" ) );
            statement.relationshipDelete( rel );

            // When
            try
            {
                statement.relationshipRemoveProperty( rel, prop1 );
                fail( "Should have failed." );
            }
            catch ( EntityNotFoundException e )
            {
                assertThat( e.getMessage(),
                            equalTo( "Unable to load RELATIONSHIP with id " + rel + "." ) );
            }
        }
    }

    @Test
    public void shouldBeAbleToRemoveResetAndTwiceRemovePropertyOnNode() throws Exception
    {
        // given
        long node;
        int prop;
        {
            DataWriteOperations ops = dataWriteOperationsInNewTransaction();
            prop = ops.propertyKeyGetOrCreateForName( "foo" );

            node = ops.nodeCreate();
            ops.nodeSetProperty( node, property( prop, "bar" ) );

            commit();
        }

        // when
        {
            DataWriteOperations ops = dataWriteOperationsInNewTransaction();
            ops.nodeRemoveProperty( node, prop );
            ops.nodeSetProperty( node, property( prop, "bar" ) );
            ops.nodeRemoveProperty( node, prop );
            ops.nodeRemoveProperty( node, prop );

            commit();
        }

        // then
        {
            ReadOperations ops = readOperationsInNewTransaction();
            assertThat( ops.nodeGetProperty( node, prop ), nullValue() );
        }
    }

    @Test
    public void shouldBeAbleToRemoveResetAndTwiceRemovePropertyOnRelationship() throws Exception
    {
        // given
        long rel;
        int prop;
        {
            DataWriteOperations ops = dataWriteOperationsInNewTransaction();
            prop = ops.propertyKeyGetOrCreateForName( "foo" );
            int type = ops.relationshipTypeGetOrCreateForName( "RELATED" );

            rel = ops.relationshipCreate( type, ops.nodeCreate(), ops.nodeCreate() );
            ops.relationshipSetProperty( rel, property( prop, "bar" ) );

            commit();
        }

        // when
        {
            DataWriteOperations ops = dataWriteOperationsInNewTransaction();
            ops.relationshipRemoveProperty( rel, prop );
            ops.relationshipSetProperty( rel, property( prop, "bar" ) );
            ops.relationshipRemoveProperty( rel, prop );
            ops.relationshipRemoveProperty( rel, prop );

            commit();
        }

        // then
        {
            ReadOperations ops = readOperationsInNewTransaction();
            assertThat( ops.relationshipGetProperty( rel, prop ), nullValue() );
        }
    }

    private static Matcher<Property> isDefinedProperty()
    {
        return new TypeSafeMatcher<Property>()
        {
            @Override
            protected boolean matchesSafely( Property item )
            {
                return item.isDefined();
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "a defined Property" );
            }
        };
    }
}


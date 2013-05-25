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

import java.util.Collections;
import java.util.Set;

import org.junit.Test;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.api.properties.Property;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import static org.neo4j.helpers.collection.IteratorUtil.asSet;

public class PropertyIT extends KernelIntegrationTest
{
    @Test
    public void shouldSetNodePropertyValue() throws Exception
    {
        // GIVEN
        newTransaction();
        Node node = db.createNode();

        // WHEN
        long propertyKeyId = statement.propertyKeyGetOrCreateForName( "clown" );
        long nodeId = node.getId();
        statement.nodeSetProperty( nodeId, Property.stringProperty( propertyKeyId, "bozo" ) );

        // THEN
        assertEquals( "bozo", statement.nodeGetProperty( nodeId, propertyKeyId ).value() );

        // WHEN
        commit();
        newTransaction();

        // THEN
        assertEquals( "bozo", statement.nodeGetProperty( nodeId, propertyKeyId ).value() );
    }

    @Test
    public void shouldRemoveSetNodeProperty() throws Exception
    {
        // GIVEN
        newTransaction();
        Node node = db.createNode();
        long propertyKeyId = statement.propertyKeyGetOrCreateForName( "clown" );
        long nodeId = node.getId();
        statement.nodeSetProperty( nodeId, Property.stringProperty( propertyKeyId, "bozo" ) );

        // WHEN
        statement.nodeRemoveProperty( nodeId, propertyKeyId );

        // THEN
        assertFalse( statement.nodeHasProperty( nodeId, propertyKeyId ) );

        // WHEN
        commit();

        // THEN
        newTransaction();
        assertFalse( statement.nodeHasProperty( nodeId, propertyKeyId ) );
    }

    @Test
    public void shouldRemoveSetNodePropertyAcrossTransactions() throws Exception
    {
        // GIVEN
        newTransaction();
        Node node = db.createNode();
        long propertyKeyId = statement.propertyKeyGetOrCreateForName( "clown" );
        long nodeId = node.getId();
        statement.nodeSetProperty( nodeId, Property.stringProperty( propertyKeyId, "bozo" ) );
        commit();
        newTransaction();

        // WHEN
        Object previous = statement.nodeRemoveProperty( nodeId, propertyKeyId ).value();

        // THEN
        assertEquals( "bozo", previous );
        assertFalse( "node should not have property", statement.nodeHasProperty( nodeId, propertyKeyId ) );

        // WHEN
        commit();

        // THEN
        newTransaction();
        assertFalse( statement.nodeHasProperty( nodeId, propertyKeyId ) );
    }

    @Test
    public void shouldSilentlyNotRemoveMissingNodeProperty() throws Exception
    {
        // GIVEN
        newTransaction();
        Node node = db.createNode();
        long propertyId = statement.propertyKeyGetOrCreateForName( "clown" );
        long nodeId = node.getId();
        commit();
        newTransaction();

        // WHEN
        Property result = statement.nodeRemoveProperty( nodeId, propertyId );

        // THEN
        assertTrue( "Return no property if removing missing", result.isNoProperty() );
    }

    @Test
    public void nodeHasPropertyIfSet() throws Exception
    {
        // GIVEN
        newTransaction();
        Node node = db.createNode();

        // WHEN
        long propertyKeyId = statement.propertyKeyGetOrCreateForName( "clown" );
        long nodeId = node.getId();
        statement.nodeSetProperty( nodeId, Property.stringProperty( propertyKeyId, "bozo" ) );

        // THEN
        assertTrue( statement.nodeHasProperty( nodeId, propertyKeyId ) );

        // WHEN
        commit();
        newTransaction();

        // THEN
        assertTrue( statement.nodeHasProperty( nodeId, propertyKeyId ) );
    }

    @Test
    public void nodeHasNotPropertyIfUnset() throws Exception
    {
        // GIVEN
        newTransaction();
        Node node = db.createNode();

        // WHEN
        long propertyId = statement.propertyKeyGetOrCreateForName( "clown" );
        long nodeId = node.getId();

        // THEN
        assertFalse( statement.nodeHasProperty( nodeId, propertyId ) );

        // WHEN
        commit();
        newTransaction();

        // THEN
        assertFalse( statement.nodeHasProperty( nodeId, propertyId ) );
    }

    @Test
    public void shouldRollbackSetNodePropertyValue() throws Exception
    {
        // GIVEN
        newTransaction();
        Node node = db.createNode();
        long propertyKeyId = statement.propertyKeyGetOrCreateForName( "clown" );
        long nodeId = node.getId();
        commit();

        // WHEN
        newTransaction();
        statement.nodeSetProperty( nodeId, Property.stringProperty( propertyKeyId, "bozo" ) );
        rollback();

        // THEN
        newTransaction();
        assertFalse( statement.nodeHasProperty( nodeId, propertyKeyId ) );
    }

    @Test
    public void shouldUpdateNodePropertyValue() throws Exception
    {
        // GIVEN
        newTransaction();
        Node node = db.createNode();
        long propertyId = statement.propertyKeyGetOrCreateForName( "clown" );
        long nodeId = node.getId();
        statement.nodeSetProperty( nodeId, Property.stringProperty( propertyId, "bozo" ) );
        commit();

        // WHEN
        newTransaction();
        statement.nodeSetProperty( nodeId, Property.intProperty( propertyId, 42 ) );
        commit();

        // THEN
        newTransaction();
        assertEquals( 42, statement.nodeGetProperty( nodeId, propertyId ).value() );
    }

    @Test
    public void shouldListNodePropertyKeys() throws Exception
    {
        // WHEN
        newTransaction();
        Node node = db.createNode();
        node.setProperty( "prop", "value" );

        // THEN
        assertThat( asSet( statement.nodeGetPropertyKeys( node.getId() ) ),
                equalTo( asSet( statement.propertyKeyGetForName( "prop" ) ) ) );

        // WHEN
        commit();

        // THEN
        newTransaction();
        assertThat( asSet( statement.nodeGetPropertyKeys( node.getId() ) ),
                equalTo( asSet( statement.propertyKeyGetForName( "prop" ) ) ) );
        commit();

        // WHEN
        newTransaction();
        node.removeProperty( "prop" );

        // THEN
        assertThat( asSet( statement.nodeGetPropertyKeys( node.getId() ) ),
                equalTo( Collections.<Long>emptySet() ) );

        // WHEN
        commit();

        // THEN
        newTransaction();
        assertThat( asSet( statement.nodeGetPropertyKeys( node.getId() ) ),
                equalTo( Collections.<Long>emptySet() ) );
        commit();
    }

    @Test
    public void shouldListRelationshipPropertyKeys() throws Exception
    {
        // WHEN
        newTransaction();
        Relationship rel = db.createNode().createRelationshipTo( db.createNode(), DynamicRelationshipType.withName( "Lol" ) );
        rel.setProperty( "prop", "value" );

        // THEN
        Set<Long> actualKeys = asSet( statement.relationshipGetPropertyKeys( rel.getId() ) );
        assertThat( actualKeys, equalTo( asSet( statement.propertyKeyGetForName( "prop" ) ) ) );

        // WHEN
        commit();

        // THEN
        newTransaction();
        actualKeys = asSet( statement.relationshipGetPropertyKeys( rel.getId() ) );
        assertThat( actualKeys, equalTo( asSet( statement.propertyKeyGetForName( "prop" ) ) ) );
        commit();

        // WHEN
        newTransaction();
        rel.removeProperty( "prop" );

        // THEN
        actualKeys = asSet( statement.relationshipGetPropertyKeys( rel.getId() ) );
        assertThat( actualKeys, equalTo( Collections.<Long>emptySet() ) );

        // WHEN
        commit();

        // THEN
        newTransaction();
        actualKeys = asSet( statement.relationshipGetPropertyKeys( rel.getId() ) );
        assertThat( actualKeys, equalTo( Collections.<Long>emptySet() ) );
        commit();
    }
}

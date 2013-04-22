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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

import java.util.Collections;

import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class PropertyIT extends KernelIntegrationTest
{
    @Test
    public void shouldSetNodePropertyValue() throws Exception
    {
        // GIVEN
        newTransaction();
        Node node = db.createNode();

        // WHEN
        long propertyId = statement.getOrCreatePropertyKeyId( "clown" );
        long nodeId = node.getId();
        statement.nodeSetPropertyValue( nodeId, propertyId, "bozo" );

        // THEN
        assertEquals( "bozo", statement.getNodePropertyValue( nodeId, propertyId ) );

        // WHEN
        commit();
        newTransaction();

        // THEN
        assertEquals( "bozo", statement.getNodePropertyValue( nodeId, propertyId ) );
    }

    @Test
    public void shouldRemoveSetNodeProperty() throws Exception
    {
        // GIVEN
        newTransaction();
        Node node = db.createNode();
        long propertyId = statement.getOrCreatePropertyKeyId( "clown" );
        long nodeId = node.getId();
        statement.nodeSetPropertyValue( nodeId, propertyId, "bozo" );

        // WHEN
        statement.nodeRemoveProperty( nodeId, propertyId );

        // THEN
        assertFalse( statement.nodeHasProperty( nodeId, propertyId ) );

        // WHEN
        commit();

        // THEN
        newTransaction();
        assertFalse( statement.nodeHasProperty( nodeId, propertyId ) );
    }

    @Test
    public void shouldRemoveSetNodePropertyAcrossTransactions() throws Exception
    {
        // GIVEN
        newTransaction();
        Node node = db.createNode();
        long propertyId = statement.getOrCreatePropertyKeyId( "clown" );
        long nodeId = node.getId();
        statement.nodeSetPropertyValue( nodeId, propertyId, "bozo" );
        commit();
        newTransaction();

        // WHEN
        Object previous = statement.nodeRemoveProperty( nodeId, propertyId );

        // THEN
        assertEquals( "bozo", previous );
        assertFalse( statement.nodeHasProperty( nodeId, propertyId ) );

        // WHEN
        commit();

        // THEN
        newTransaction();
        assertFalse( statement.nodeHasProperty( nodeId, propertyId ) );
    }

    @Test
    public void shouldSilentlyNotRemoveMissingNodeProperty() throws Exception
    {
        // GIVEN
        newTransaction();
        Node node = db.createNode();
        long propertyId = statement.getOrCreatePropertyKeyId( "clown" );
        long nodeId = node.getId();
        commit();
        newTransaction();

        // WHEN
        Object result = statement.nodeRemoveProperty( nodeId, propertyId );

        // THEN
        assertEquals( null, result );
    }

    @Test
    public void nodeHasPropertyIfSet() throws Exception
    {
        // GIVEN
        newTransaction();
        Node node = db.createNode();

        // WHEN
        long propertyId = statement.getOrCreatePropertyKeyId( "clown" );
        long nodeId = node.getId();
        statement.nodeSetPropertyValue( nodeId, propertyId, "bozo" );

        // THEN
        assertTrue( statement.nodeHasProperty( nodeId, propertyId ) );

        // WHEN
        commit();
        newTransaction();

        // THEN
        assertTrue( statement.nodeHasProperty( nodeId, propertyId ) );
    }

    @Test
    public void nodeHasNotPropertyIfUnset() throws Exception
    {
        // GIVEN
        newTransaction();
        Node node = db.createNode();

        // WHEN
        long propertyId = statement.getOrCreatePropertyKeyId( "clown" );
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
        long propertyId = statement.getOrCreatePropertyKeyId( "clown" );
        long nodeId = node.getId();
        commit();

        // WHEN
        newTransaction();
        statement.nodeSetPropertyValue( nodeId, propertyId, "bozo" );
        rollback();

        // THEN
        newTransaction();
        assertFalse( statement.nodeHasProperty( nodeId, propertyId ) );
    }

    @Test
    public void shouldUpdateNodePropertyValue() throws Exception
    {
        // GIVEN
        newTransaction();
        Node node = db.createNode();
        long propertyId = statement.getOrCreatePropertyKeyId( "clown" );
        long nodeId = node.getId();
        statement.nodeSetPropertyValue( nodeId, propertyId, "bozo" );
        commit();

        // WHEN
        newTransaction();
        statement.nodeSetPropertyValue( nodeId, propertyId, 42 );
        commit();

        // THEN
        newTransaction();
        assertEquals( 42, statement.getNodePropertyValue( nodeId, propertyId ) );
    }

    @Test
    public void shouldListNodePropertyKeys() throws Exception
    {
        // WHEN
        newTransaction();
        Node node = db.createNode();
        node.setProperty( "prop", "value" );

        // THEN
        assertThat( asSet( statement.listNodePropertyKeys( node.getId() ) ),
                equalTo( asSet( statement.getPropertyKeyId( "prop" ) ) ) );

        // WHEN
        commit();

        // THEN
        newTransaction();
        assertThat( asSet( statement.listNodePropertyKeys( node.getId() ) ),
                equalTo( asSet( statement.getPropertyKeyId( "prop" ) ) ) );
        commit();

        // WHEN
        newTransaction();
        node.removeProperty( "prop" );

        // THEN
        assertThat( asSet( statement.listNodePropertyKeys( node.getId() ) ),
                equalTo( Collections.<Long>emptySet() ) );

        // WHEN
        commit();

        // THEN
        newTransaction();
        assertThat( asSet( statement.listNodePropertyKeys( node.getId() ) ),
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
        assertThat( asSet( statement.listRelationshipPropertyKeys( rel.getId() ) ),
                equalTo( asSet( statement.getPropertyKeyId( "prop" ) ) ) );

        // WHEN
        commit();

        // THEN
        newTransaction();
        assertThat( asSet( statement.listRelationshipPropertyKeys( rel.getId() ) ),
                equalTo( asSet( statement.getPropertyKeyId( "prop" ) ) ) );
        commit();

        // WHEN
        newTransaction();
        rel.removeProperty( "prop" );

        // THEN
        assertThat( asSet( statement.listRelationshipPropertyKeys( rel.getId() ) ),
                equalTo( Collections.<Long>emptySet() ) );

        // WHEN
        commit();

        // THEN
        newTransaction();
        assertThat( asSet( statement.listRelationshipPropertyKeys( rel.getId() ) ),
                equalTo( Collections.<Long>emptySet() ) );
        commit();
    }
}

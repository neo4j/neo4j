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
package org.neo4j.internal.kernel.api;

import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.Iterables;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.stringValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings( "Duplicates" )
public abstract class RelationshipWriteTestBase<G extends KernelAPIWriteTestSupport> extends KernelAPIWriteTestBase<G>
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    void shouldCreateRelationship() throws Exception
    {
        long n1, n2;
        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            n1 = graphDb.createNode().getId();
            n2 = graphDb.createNode().getId();
            tx.success();
        }

        long r;
        try ( Transaction tx = session.beginTransaction() )
        {
            int label = tx.token().relationshipTypeGetOrCreateForName( "R" );
            r = tx.dataWrite().relationshipCreate( n1, label, n2 );
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            List<Relationship> relationships = Iterables.asList( graphDb.getNodeById( n1 ).getRelationships() );
            assertEquals( 1, relationships.size() );
            assertEquals( relationships.get( 0 ).getId(), r );
        }
    }

    @Test
    void shouldCreateRelationshipBetweenInTransactionNodes() throws Exception
    {
        long n1, n2, r;
        try ( Transaction tx = session.beginTransaction() )
        {
            n1 = tx.dataWrite().nodeCreate();
            n2 = tx.dataWrite().nodeCreate();
            int label = tx.token().relationshipTypeGetOrCreateForName( "R" );
            r = tx.dataWrite().relationshipCreate( n1, label, n2 );
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            List<Relationship> relationships = Iterables.asList( graphDb.getNodeById( n1 ).getRelationships() );
            assertEquals( 1, relationships.size() );
            assertEquals( relationships.get( 0 ).getId(), r );
        }
    }

    @Test
    void shouldRollbackRelationshipOnFailure() throws Exception
    {
        long n1, n2;
        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            n1 = graphDb.createNode().getId();
            n2 = graphDb.createNode().getId();
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            int label = tx.token().relationshipTypeGetOrCreateForName( "R" );
            tx.dataWrite().relationshipCreate( n1, label, n2 );
            tx.failure();
        }

        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            assertEquals( 0, graphDb.getNodeById( n1 ).getDegree() );
        }
    }

    @Test
    void shouldDeleteRelationship() throws Exception
    {
        long n1, r;
        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            Node node1 = graphDb.createNode();
            Node node2 = graphDb.createNode();

            n1 = node1.getId();
            r = node1.createRelationshipTo( node2, RelationshipType.withName( "R" ) ).getId();

            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            assertTrue( tx.dataWrite().relationshipDelete( r ), "should delete relationship" );
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            assertEquals( 0, graphDb.getNodeById( n1 ).getDegree() );
        }
    }

    @Test
    void shouldNotDeleteRelationshipThatDoesNotExist() throws Exception
    {
        long relationship = 0;

        try ( Transaction tx = session.beginTransaction() )
        {
            assertFalse( tx.dataWrite().relationshipDelete( relationship ) );
            tx.failure();
        }
        try ( Transaction tx = session.beginTransaction() )
        {
            assertFalse( tx.dataWrite().relationshipDelete( relationship ) );
            tx.success();
        }
        // should not crash
    }

    @Test
    void shouldDeleteRelationshipAddedInTransaction() throws Exception
    {
        long n1, n2;
        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            n1 = graphDb.createNode().getId();
            n2 = graphDb.createNode().getId();
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            int label = tx.token().relationshipTypeGetOrCreateForName( "R" );
            long r = tx.dataWrite().relationshipCreate( n1, label, n2 );

            assertTrue( tx.dataWrite().relationshipDelete( r ) );
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            assertEquals( 0, graphDb.getNodeById( n1 ).getDegree() );
        }
    }

    @Test
    public void shouldAddPropertyToRelationship() throws Exception
    {
        // Given
        long  relationshipId;
        String propertyKey = "prop";
        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            Node node1 = graphDb.createNode();
            Node node2 = graphDb.createNode();

            relationshipId = node1.createRelationshipTo( node2, RelationshipType.withName( "R" ) ).getId();

            tx.success();
        }

        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            int token = tx.token().propertyKeyGetOrCreateForName( propertyKey );
            assertThat( tx.dataWrite().relationshipSetProperty( relationshipId, token, stringValue( "hello" ) ), equalTo( NO_VALUE ) );
            tx.success();
        }

        // Then
        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            assertThat( graphDb.getRelationshipById( relationshipId ).getProperty( "prop" ), equalTo( "hello" ) );
        }
    }

    @Test
    public void shouldUpdatePropertyToRelationship() throws Exception
    {
        // Given
        long  relationshipId;
        String propertyKey = "prop";
        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            Node node1 = graphDb.createNode();
            Node node2 = graphDb.createNode();

            Relationship r = node1.createRelationshipTo( node2, RelationshipType.withName( "R" ) );
            r.setProperty( propertyKey, 42  );
            relationshipId = r.getId();

            tx.success();
        }

        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            int token = tx.token().propertyKeyGetOrCreateForName( propertyKey );
            assertThat( tx.dataWrite().relationshipSetProperty( relationshipId, token, stringValue( "hello" ) ),
                    equalTo( intValue( 42 ) ) );
            tx.success();
        }

        // Then
        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            assertThat( graphDb.getRelationshipById( relationshipId ).getProperty( "prop" ), equalTo( "hello" ) );
        }
    }

    @Test
    public void shouldRemovePropertyFromRelationship() throws Exception
    {
        // Given
        long  relationshipId;
        String propertyKey = "prop";
        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            Node node1 = graphDb.createNode();
            Node node2 = graphDb.createNode();

            Relationship proxy = node1.createRelationshipTo( node2, RelationshipType.withName( "R" ) );
            relationshipId = proxy.getId();
            proxy.setProperty( propertyKey, 42 );
            tx.success();
        }

        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            int token = tx.token().propertyKeyGetOrCreateForName( propertyKey );
            assertThat( tx.dataWrite().relationshipRemoveProperty( relationshipId, token ),
                    equalTo( intValue( 42 ) ) );
            tx.success();
        }

        // Then
        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            assertFalse( graphDb.getRelationshipById( relationshipId ).hasProperty( "prop" ) );
        }
    }

    @Test
    public void shouldRemoveNonExistingPropertyFromRelationship() throws Exception
    {
        // Given
        long  relationshipId;
        String propertyKey = "prop";
        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            Node node1 = graphDb.createNode();
            Node node2 = graphDb.createNode();

            Relationship proxy = node1.createRelationshipTo( node2, RelationshipType.withName( "R" ) );
            relationshipId = proxy.getId();
            tx.success();
        }
        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            int token = tx.token().propertyKeyGetOrCreateForName( propertyKey );
            assertThat( tx.dataWrite().relationshipRemoveProperty( relationshipId, token ),
                    equalTo( NO_VALUE ) );
            tx.success();
        }

        // Then
        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            assertFalse( graphDb.getRelationshipById( relationshipId ).hasProperty( "prop" ) );
        }
    }

    @Test
    public void shouldRemovePropertyFromRelationshipTwice() throws Exception
    {
        // Given
        long  relationshipId;
        String propertyKey = "prop";
        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            Node node1 = graphDb.createNode();
            Node node2 = graphDb.createNode();

            Relationship proxy = node1.createRelationshipTo( node2, RelationshipType.withName( "R" ) );
            relationshipId = proxy.getId();
            proxy.setProperty( propertyKey, 42 );
            tx.success();
        }

        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            int token = tx.token().propertyKeyGetOrCreateForName( propertyKey );
            assertThat( tx.dataWrite().relationshipRemoveProperty( relationshipId, token ),
                    equalTo( intValue( 42 ) ) );
            assertThat( tx.dataWrite().relationshipRemoveProperty( relationshipId, token ),
                    equalTo( NO_VALUE ) );
            tx.success();
        }

        // Then
        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            assertFalse( graphDb.getRelationshipById( relationshipId ).hasProperty( "prop" ) );
        }
    }

    @Test
    public void shouldUpdatePropertyToRelationshipInTransaction() throws Exception
    {
        // Given
        long  relationshipId;
        String propertyKey = "prop";
        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            Node node1 = graphDb.createNode();
            Node node2 = graphDb.createNode();

            relationshipId = node1.createRelationshipTo( node2, RelationshipType.withName( "R" ) ).getId();

            tx.success();
        }

        // When
        try ( Transaction tx = session.beginTransaction() )
        {
            int token = tx.token().propertyKeyGetOrCreateForName( propertyKey );
            assertThat( tx.dataWrite().relationshipSetProperty( relationshipId, token, stringValue( "hello" ) ), equalTo( NO_VALUE ) );
            assertThat( tx.dataWrite().relationshipSetProperty( relationshipId, token, stringValue( "world" ) ), equalTo( stringValue( "hello" ) ) );
            assertThat( tx.dataWrite().relationshipSetProperty( relationshipId, token, intValue( 1337 ) ), equalTo( stringValue( "world" ) ) );
            tx.success();
        }

        // Then
        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            assertThat( graphDb.getRelationshipById( relationshipId ).getProperty( "prop" ), equalTo( 1337 ) );
        }
    }
}

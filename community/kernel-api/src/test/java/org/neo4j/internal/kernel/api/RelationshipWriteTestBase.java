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

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.stringValue;

@SuppressWarnings( "Duplicates" )
public abstract class RelationshipWriteTestBase<G extends KernelAPIWriteTestSupport> extends KernelAPIWriteTestBase<G>
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldCreateRelationship() throws Exception
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
            int label = session.token().relationshipTypeGetOrCreateForName( "R" );
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
    public void shouldCreateRelationshipBetweenInTransactionNodes() throws Exception
    {
        long n1, n2, r;
        try ( Transaction tx = session.beginTransaction() )
        {
            n1 = tx.dataWrite().nodeCreate();
            n2 = tx.dataWrite().nodeCreate();
            int label = session.token().relationshipTypeGetOrCreateForName( "R" );
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
    public void shouldRollbackRelationshipOnFailure() throws Exception
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
            int label = session.token().relationshipTypeGetOrCreateForName( "R" );
            tx.dataWrite().relationshipCreate( n1, label, n2 );
            tx.failure();
        }

        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            assertEquals( 0, graphDb.getNodeById( n1 ).getDegree() );
        }
    }

    @Test
    public void shouldDeleteRelationship() throws Exception
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
            assertTrue( "should delete relationship", tx.dataWrite().relationshipDelete( r ) );
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            assertEquals( 0, graphDb.getNodeById( n1 ).getDegree() );
        }
    }

    @Test
    public void shouldNotDeleteRelationshipThatDoesNotExist() throws Exception
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
    public void shouldDeleteRelationshipAddedInTransaction() throws Exception
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
            int label = session.token().relationshipTypeGetOrCreateForName( "R" );
            long r = tx.dataWrite().relationshipCreate( n1, label, n2 );

            assertTrue( tx.dataWrite().relationshipDelete( r ) );
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            assertEquals( 0, graphDb.getNodeById( n1 ).getDegree() );
        }
    }
}

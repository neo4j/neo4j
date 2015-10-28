/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.graphdb;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.embedded.TestGraphDatabase;
import org.neo4j.test.EphemeralFileSystemRule;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class IdReuseTest
{
    @Rule
    public final EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    public TestGraphDatabase db;

    @Before
    public void before()
    {
        startDB();
    }

    private TestGraphDatabase startDB()
    {
        assert (db == null);
        db = TestGraphDatabase.buildEphemeral().withFileSystem( fsRule.get() ).open();
        return db;
    }

    private TestGraphDatabase restartDB()
    {
        db.shutdown();
        db = null;
        return startDB();
    }

    public void after()
    {
        if ( db != null )
        {
            db.shutdown();
        }
    }

    @Test
    public void shouldReuseNodeIdsFromRolledBackTransaction() throws Exception
    {
        // Given
        try (Transaction tx = db.beginTx())
        {
            db.createNode();

            tx.failure();
        }

        db = restartDB();

        // When
        Node node;
        try (Transaction tx = db.beginTx())
        {
            node = db.createNode();

            tx.success();
        }

        // Then
        assertThat(node.getId(), equalTo(0L));
    }

    @Test
    public void shouldReuseRelationshipIdsFromRolledBackTransaction() throws Exception
    {
        // Given
        Node node1, node2;
        try (Transaction tx = db.beginTx())
        {
            node1 = db.createNode();
            node2 = db.createNode();

            tx.success();
        }

        try (Transaction tx = db.beginTx())
        {
            node1.createRelationshipTo( node2, DynamicRelationshipType.withName( "LIKE" ) );

            tx.failure();
        }

        db = restartDB();

        // When
        Relationship relationship;
        try (Transaction tx = db.beginTx())
        {
            node1 = db.getNodeById(node1.getId());
            node2 = db.getNodeById(node2.getId());
            relationship = node1.createRelationshipTo( node2, DynamicRelationshipType.withName( "LIKE" ) );

            tx.success();
        }

        // Then
        assertThat(relationship.getId(), equalTo(0L));
    }
}

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
package org.neo4j.kernel.impl.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterators.addToCollection;

class NodeManagerTest
{
    private GraphDatabaseAPI db;
    private DatabaseManagementService managementService;

    @BeforeEach
    void init()
    {
        managementService = new TestDatabaseManagementServiceBuilder().impermanent().build();
        db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
    }

    @AfterEach
    void stop()
    {
        managementService.shutdown();
    }

    @Test
    void getAllNodesIteratorShouldPickUpHigherIdsThanHighIdWhenStarted() throws Exception
    {
        // GIVEN
        {
            Transaction tx = db.beginTx();
            db.createNode();
            db.createNode();
            tx.commit();
        }

        // WHEN iterator is started
        Transaction transaction = db.beginTx();
        Iterator<Node> allNodes = db.getAllNodes().iterator();
        allNodes.next();

        // and WHEN another node is then added
        Thread thread = new Thread( () ->
        {
            Transaction newTx = db.beginTx();
            db.createNode();
            newTx.commit();
        } );
        thread.start();
        thread.join();

        // THEN the new node is picked up by the iterator
        assertThat( addToCollection( allNodes, new ArrayList<>() ).size(), is( 2 ) );
        transaction.close();
    }

    @Test
    void getAllRelationshipsIteratorShouldPickUpHigherIdsThanHighIdWhenStarted() throws Exception
    {
        // GIVEN
        Transaction tx = db.beginTx();
        createRelationshipAssumingTxWith( "key", 1 );
        createRelationshipAssumingTxWith( "key", 2 );
        tx.commit();

        // WHEN
        tx = db.beginTx();
        Iterator<Relationship> allRelationships = db.getAllRelationships().iterator();

        Thread thread = new Thread( () ->
        {
            Transaction newTx = db.beginTx();
            createRelationshipAssumingTxWith( "key", 3 );
            newTx.commit();
        } );
        thread.start();
        thread.join();

        // THEN
        assertThat( addToCollection( allRelationships, new ArrayList<>() ).size(), is(3) );
        tx.commit();
    }

    private void createRelationshipAssumingTxWith( String key, Object value )
    {
        Node a = db.createNode();
        Node b = db.createNode();
        Relationship relationship = a.createRelationshipTo( b, RelationshipType.withName( "FOO" ) );
        relationship.setProperty( key, value );
    }
}

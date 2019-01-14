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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.coreapi.PlaceboTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.Iterators.addToCollection;

public class NodeManagerTest
{
    private GraphDatabaseAPI db;

    @Before
    public void init()
    {
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
    }

    @After
    public void stop()
    {
        db.shutdown();
    }

    @Test
    public void getAllNodesIteratorShouldPickUpHigherIdsThanHighIdWhenStarted() throws Exception
    {
        // GIVEN
        {
            Transaction tx = db.beginTx();
            db.createNode();
            db.createNode();
            tx.success();
            tx.close();
        }

        // WHEN iterator is started
        Transaction transaction = db.beginTx();
        Iterator<Node> allNodes = db.getAllNodes().iterator();
        allNodes.next();

        // and WHEN another node is then added
        Thread thread = new Thread( () ->
        {
            Transaction newTx = db.beginTx();
            assertThat( newTx, not( instanceOf( PlaceboTransaction.class ) ) );
            db.createNode();
            newTx.success();
            newTx.close();
        } );
        thread.start();
        thread.join();

        // THEN the new node is picked up by the iterator
        assertThat( addToCollection( allNodes, new ArrayList<>() ).size(), is( 2 ) );
        transaction.close();
    }

    @Test
    public void getAllRelationshipsIteratorShouldPickUpHigherIdsThanHighIdWhenStarted() throws Exception
    {
        // GIVEN
        Transaction tx = db.beginTx();
        createRelationshipAssumingTxWith( "key", 1 );
        createRelationshipAssumingTxWith( "key", 2 );
        tx.success();
        tx.close();

        // WHEN
        tx = db.beginTx();
        Iterator<Relationship> allRelationships = db.getAllRelationships().iterator();

        Thread thread = new Thread( () ->
        {
            Transaction newTx = db.beginTx();
            assertThat( newTx, not( instanceOf( PlaceboTransaction.class ) ) );
            createRelationshipAssumingTxWith( "key", 3 );
            newTx.success();
            newTx.close();
        } );
        thread.start();
        thread.join();

        // THEN
        assertThat( addToCollection( allRelationships, new ArrayList<>() ).size(), is(3) );
        tx.success();
        tx.close();
    }

    private Relationship createRelationshipAssumingTxWith( String key, Object value )
    {
        Node a = db.createNode();
        Node b = db.createNode();
        Relationship relationship = a.createRelationshipTo( b, RelationshipType.withName( "FOO" ) );
        relationship.setProperty( key, value );
        return relationship;
    }
}

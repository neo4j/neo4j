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
package org.neo4j.kernel.api.impl.index;

import java.util.Arrays;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema.IndexState;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertThat;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.Neo4jMatchers.containsOnly;
import static org.neo4j.graphdb.Neo4jMatchers.createIndex;
import static org.neo4j.graphdb.Neo4jMatchers.findNodesByLabelAndProperty;
import static org.neo4j.graphdb.Neo4jMatchers.getIndexes;
import static org.neo4j.graphdb.Neo4jMatchers.haveState;
import static org.neo4j.graphdb.Neo4jMatchers.inTx;
import static org.neo4j.graphdb.Neo4jMatchers.isEmpty;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.helpers.collection.IteratorUtil.loop;
import static org.neo4j.helpers.collection.MapUtil.map;

public class SchemaIndexAcceptanceTest
{
    @Test
    public void creatingIndexOnExistingDataBuildsIndexWhichWillBeOnlineNextStartup() throws Exception
    {
        Node node1;
        Node node2;
        Node node3;
        try ( Transaction tx = db.beginTx() )
        {
            node1 = createNode( label, "name", "One" );
            node2 = createNode( label, "name", "Two" );
            node3 = createNode( label, "name", "Three" );
            tx.success();
        }

        createIndex( db, label, propertyKey );

        restart();

        assertThat( findNodesByLabelAndProperty( label, "name", "One", db ), containsOnly( node1 ) );
        assertThat( findNodesByLabelAndProperty( label, "name", "Two", db ), containsOnly( node2 ) );
        assertThat( findNodesByLabelAndProperty( label, "name", "Three", db ), containsOnly( node3 ) );
    }

    @Test
    public void shouldIndexArrays() throws Exception
    {
        long[] arrayPropertyValue = {42, 23, 87};
        createIndex( db, label, propertyKey );
        Node node1;
        try ( Transaction tx = db.beginTx() )
        {
            node1 = createNode( label, propertyKey, arrayPropertyValue );
            tx.success();
        }

        restart();

        assertThat( getIndexes( db, label ), inTx( db, haveState( db, IndexState.ONLINE ) ));
        assertThat( findNodesByLabelAndProperty( label, propertyKey, arrayPropertyValue, db ), containsOnly( node1 ) );
        assertThat( findNodesByLabelAndProperty( label, propertyKey, new long[]{42, 23}, db ), isEmpty() );
        assertThat( findNodesByLabelAndProperty( label, propertyKey, Arrays.toString( arrayPropertyValue ), db ), isEmpty() );
    }

    @Test
    public void shouldIndexStringArrays() throws Exception
    {
        String[] arrayPropertyValue = {"A, B", "C"};
        createIndex( db, label, propertyKey );
        Node node1;
        try ( Transaction tx = db.beginTx() )
        {
            node1 = createNode( label, propertyKey, arrayPropertyValue );
            tx.success();
        }

        restart();

        assertThat( getIndexes( db, label ), inTx( db, haveState( db, IndexState.ONLINE ) ) );
        assertThat( findNodesByLabelAndProperty( label, propertyKey, arrayPropertyValue, db ), containsOnly( node1 ) );
        assertThat( findNodesByLabelAndProperty( label, propertyKey, new String[]{"A", "B, C"}, db ), isEmpty() );
        assertThat( findNodesByLabelAndProperty( label, propertyKey, Arrays.toString( arrayPropertyValue ), db ), isEmpty() );

    }

    @Test
    public void shouldIndexArraysPostPopulation() throws Exception
    {
        long[] arrayPropertyValue = {42, 23, 87};
        Node node1;
        try ( Transaction tx = db.beginTx() )
        {
            node1 = createNode( label, propertyKey, arrayPropertyValue );
            tx.success();
        }

        createIndex( db, label, propertyKey );

        restart();

        assertThat( getIndexes( db, label ), inTx( db, haveState( db, IndexState.ONLINE ) ) );
        assertThat( findNodesByLabelAndProperty( label, propertyKey, arrayPropertyValue, db ), containsOnly( node1 ) );
        assertThat( findNodesByLabelAndProperty( label, propertyKey, new long[]{42, 23}, db ), isEmpty() );
        assertThat( findNodesByLabelAndProperty( label, propertyKey, Arrays.toString( arrayPropertyValue ), db ), isEmpty() );
    }

    @Test
    public void recoveryAfterCreateAndDropIndex() throws Exception
    {
        // GIVEN
        IndexDefinition indexDefinition = createIndex( db, label, propertyKey );
        createSomeData( label, propertyKey );
        doStuff( db, label, propertyKey );
        dropIndex( indexDefinition );
        doStuff( db, label, propertyKey );

        // WHEN
        crashAndRestart();

        // THEN
        assertThat( getIndexes( db, label ), isEmpty() );
    }

    private EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
    private GraphDatabaseService db;
    private final Label label = label( "PERSON" );
    private final String propertyKey = "key";

    @Before
    public void before() throws Exception
    {
        db = newDb();
    }

    private GraphDatabaseService newDb()
    {
        return new TestGraphDatabaseFactory().setFileSystem( fs ).newImpermanentDatabase();
    }

    private void crashAndRestart()
    {
        EphemeralFileSystemAbstraction snapshot = fs.snapshot();
        db.shutdown();
        fs.shutdown();
        fs = snapshot;
        db = newDb();
    }

    private void restart()
    {
        db.shutdown();
        db = newDb();
    }

    @After
    public void after() throws Exception
    {
        db.shutdown();
    }

    private Node createNode( Label label, Object... properties )
    {
        Node node = db.createNode( label );
        for ( Map.Entry<String, Object> property : map( properties ).entrySet() )
        {
            node.setProperty( property.getKey(), property.getValue() );
        }
        return node;
    }

    private void dropIndex( IndexDefinition indexDefinition )
    {
        try ( Transaction tx = db.beginTx() )
        {
            indexDefinition.drop();
            tx.success();
        }
    }

    private static void doStuff( GraphDatabaseService db, Label label, String propertyKey )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( Node node : loop( db.findNodes( label, propertyKey, 3323 ) ) )
            {
                count( node.getLabels() );
            }
        }
    }

    private void createSomeData( Label label, String propertyKey )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( label );
            node.setProperty( propertyKey, "yeah" );
            tx.success();
        }
    }
}

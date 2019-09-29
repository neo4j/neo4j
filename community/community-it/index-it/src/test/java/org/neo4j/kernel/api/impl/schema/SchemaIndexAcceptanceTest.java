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
package org.neo4j.kernel.api.impl.schema;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema.IndexState;
import org.neo4j.io.fs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.internal.helpers.collection.Iterators.loop;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.containsOnly;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.createIndex;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.findNodesByLabelAndProperty;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.getIndexes;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.haveState;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.isEmpty;

public class SchemaIndexAcceptanceTest
{
    @Rule
    public final EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();

    private GraphDatabaseService db;
    private final Label label = label( "PERSON" );
    private final String propertyKey = "key";
    private DatabaseManagementService managementService;

    @Before
    public void before()
    {
        db = newDb();
    }

    @After
    public void after()
    {
        managementService.shutdown();
    }

    @Test
    public void creatingIndexOnExistingDataBuildsIndexWhichWillBeOnlineNextStartup()
    {
        Node node1;
        Node node2;
        Node node3;
        try ( Transaction tx = db.beginTx() )
        {
            node1 = createNode( tx, label, "name", "One" );
            node2 = createNode( tx, label, "name", "Two" );
            node3 = createNode( tx, label, "name", "Three" );
            tx.commit();
        }

        createIndex( db, label, propertyKey );

        restart();

        try ( Transaction transaction = db.beginTx() )
        {
            assertThat( findNodesByLabelAndProperty( label, "name", "One", db, transaction ), containsOnly( node1 ) );
            assertThat( findNodesByLabelAndProperty( label, "name", "Two", db, transaction ), containsOnly( node2 ) );
            assertThat( findNodesByLabelAndProperty( label, "name", "Three", db, transaction ), containsOnly( node3 ) );
        }
    }

    @Test
    public void shouldIndexArrays()
    {
        long[] arrayPropertyValue = {42, 23, 87};
        createIndex( db, label, propertyKey );
        Node node1;
        try ( Transaction tx = db.beginTx() )
        {
            node1 = createNode( tx, label, propertyKey, arrayPropertyValue );
            tx.commit();
        }

        restart();

        try ( Transaction tx = db.beginTx() )
        {
            assertThat( getIndexes( tx, label ), haveState( tx, IndexState.ONLINE ) );
        }
        try ( Transaction transaction = db.beginTx() )
        {
            assertThat( findNodesByLabelAndProperty( label, propertyKey, arrayPropertyValue, db, transaction ), containsOnly( node1 ) );
            assertThat( findNodesByLabelAndProperty( label, propertyKey, new long[]{42, 23}, db, transaction ), isEmpty() );
            assertThat( findNodesByLabelAndProperty( label, propertyKey, Arrays.toString( arrayPropertyValue ), db, transaction ), isEmpty() );
            transaction.commit();
        }
    }

    @Test
    public void shouldIndexStringArrays()
    {
        String[] arrayPropertyValue = {"A, B", "C"};
        createIndex( db, label, propertyKey );
        Node node1;
        try ( Transaction tx = db.beginTx() )
        {
            node1 = createNode( tx, label, propertyKey, arrayPropertyValue );
            tx.commit();
        }

        restart();

        try ( Transaction tx = db.beginTx() )
        {
            assertThat( getIndexes( tx, label ), haveState( tx, IndexState.ONLINE ) );
        }
        try ( Transaction transaction = db.beginTx() )
        {
            assertThat( findNodesByLabelAndProperty( label, propertyKey, arrayPropertyValue, db, transaction ), containsOnly( node1 ) );
            assertThat( findNodesByLabelAndProperty( label, propertyKey, new String[]{"A", "B, C"}, db, transaction ), isEmpty() );
            assertThat( findNodesByLabelAndProperty( label, propertyKey, Arrays.toString( arrayPropertyValue ), db, transaction ), isEmpty() );
        }

    }

    @Test
    public void shouldIndexArraysPostPopulation()
    {
        long[] arrayPropertyValue = {42, 23, 87};
        Node node1;
        try ( Transaction tx = db.beginTx() )
        {
            node1 = createNode( tx, label, propertyKey, arrayPropertyValue );
            tx.commit();
        }

        createIndex( db, label, propertyKey );

        restart();

        try ( Transaction tx = db.beginTx() )
        {
            assertThat( getIndexes( tx, label ), haveState( tx, IndexState.ONLINE ) );
        }
        try ( Transaction transaction = db.beginTx() )
        {
            assertThat( findNodesByLabelAndProperty( label, propertyKey, arrayPropertyValue, db, transaction ), containsOnly( node1 ) );
            assertThat( findNodesByLabelAndProperty( label, propertyKey, new long[]{42, 23}, db, transaction ), isEmpty() );
            assertThat( findNodesByLabelAndProperty( label, propertyKey, Arrays.toString( arrayPropertyValue ), db, transaction ), isEmpty() );
        }
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
        try ( Transaction transaction = db.beginTx() )
        {
            assertThat( getIndexes( transaction, label ), isEmpty() );
        }
    }

    private GraphDatabaseService newDb()
    {
        managementService = new TestDatabaseManagementServiceBuilder().setFileSystem(
                new UncloseableDelegatingFileSystemAbstraction( fsRule.get() ) ).impermanent().build();
        return managementService.database( DEFAULT_DATABASE_NAME );
    }

    private void crashAndRestart() throws Exception
    {
        fsRule.snapshot( managementService::shutdown );
        db = newDb();
    }

    private void restart()
    {
        managementService.shutdown();
        db = newDb();
    }

    private Node createNode( Transaction tx, Label label, Object... properties )
    {
        Node node = tx.createNode( label );
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
            tx.schema().getIndexByName( indexDefinition.getName() ).drop();
            tx.commit();
        }
    }

    private static void doStuff( GraphDatabaseService db, Label label, String propertyKey )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( Node node : loop( tx.findNodes( label, propertyKey, 3323 ) ) )
            {
                count( node.getLabels() );
            }
        }
    }

    private void createSomeData( Label label, String propertyKey )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( label );
            node.setProperty( propertyKey, "yeah" );
            tx.commit();
        }
    }
}

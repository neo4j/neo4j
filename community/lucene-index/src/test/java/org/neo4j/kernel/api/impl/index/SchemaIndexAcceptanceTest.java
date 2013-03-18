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
package org.neo4j.kernel.api.impl.index;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.asUniqueSet;
import static org.neo4j.helpers.collection.MapUtil.map;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema.IndexState;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TestGraphDatabaseFactory;

public class SchemaIndexAcceptanceTest
{
    @Test
    public void creatingIndexOnExistingDataBuildsIndexWhichWillBeOnlineNextStartup() throws Exception
    {
        Transaction tx = db.beginTx();
        Node node1 = createNode( label, "name", "One" );
        Node node2 = createNode( label, "name", "Two" );
        Node node3 = createNode( label, "name", "Three" );
        tx.success();
        tx.finish();
        
        tx = db.beginTx();
        IndexDefinition index = db.schema().indexCreator( label ).on( key ).create();
        tx.success();
        tx.finish();
        db.schema().awaitIndexOnline( index, 10, TimeUnit.SECONDS );
        
        restart();
        
        assertEquals( IndexState.ONLINE, db.schema().getIndexState( index ) );
        assertEquals( asSet( node1 ), asUniqueSet( db.findNodesByLabelAndProperty( label, "name", "One" ) ) );
        assertEquals( asSet( node2 ), asUniqueSet( db.findNodesByLabelAndProperty( label, "name", "Two" ) ) );
        assertEquals( asSet( node3 ), asUniqueSet( db.findNodesByLabelAndProperty( label, "name", "Three" ) ) );
    }
    
    @Test
    public void recoveryAfterCreateAndDropIndex() throws Exception
    {
        // GIVEN
        Label label = label( "PERSON" );
        String propertyKey = "id";
        IndexDefinition indexDefinition = createIndex( label, propertyKey );
        createSomeData( label, propertyKey );
        doStuff( db, label, propertyKey );
        dropIndex( indexDefinition );
        doStuff( db, label, propertyKey );
        
        // WHEN
        crashAndRestart();
        
        // THEN
        assertEquals( asSet(), asSet( db.schema().getIndexes( label ) ) );
    }
 
    @Rule public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private GraphDatabaseService db;
    private final Label label = label( "label" );
    private final String key = "key";
    
    @Before
    public void before() throws Exception
    {
        db = newDb();
    }

    private GraphDatabaseService newDb()
    {
        return new TestGraphDatabaseFactory().setFileSystem( fs.get() ).newImpermanentDatabase();
    }
    
    private void crashAndRestart()
    {
        fs.snapshot( new Runnable()
        {
            @Override
            public void run()
            {
                db.shutdown();
            }
        } );
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
            node.setProperty( property.getKey(), property.getValue() );
        return node;
    }

    private void dropIndex( IndexDefinition indexDefinition )
    {
        Transaction tx = db.beginTx();
        indexDefinition.drop();
        tx.success();
        tx.finish();
    }

    private IndexDefinition createIndex( Label label, String propertyKey )
    {
        Transaction tx = db.beginTx();
        IndexDefinition indexDefinition = db.schema().indexCreator( label ).on( propertyKey ).create();
        tx.success();
        tx.finish();
        db.schema().awaitIndexOnline( indexDefinition, 1, MINUTES );
        return indexDefinition;
    }

    private static void doStuff( GraphDatabaseService db, Label label, String propertyKey )
    {
        Iterable<Node> nodes = db.findNodesByLabelAndProperty( label, propertyKey, 3323 );
        for ( Node node : nodes )
            count( node.getLabels() );
    }

    private void createSomeData( Label label, String propertyKey )
    {
        Transaction tx = db.beginTx();
        try
        {
            Node node = db.createNode( label );
            node.setProperty( propertyKey, "yeah" );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }
}

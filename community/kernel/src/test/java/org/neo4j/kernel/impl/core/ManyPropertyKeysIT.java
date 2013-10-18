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
package org.neo4j.kernel.impl.core;

import java.io.File;

import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;

/**
 * Tests for handling many property keys (even after restart of database)
 * as well as concurrent creation of property keys.
 */
public class ManyPropertyKeysIT
{
    @Test
    public void creating_many_property_keys_should_have_all_loaded_the_next_restart() throws Exception
    {
        // GIVEN
        GraphDatabaseAPI db = databaseWithManyPropertyKeys( 3000 ); // The previous limit to load was 2500, so go some above that
        int countBefore = propertyKeyCount( db );

        // WHEN
        db.shutdown();
        db = database();
        createNodeWithProperty( db, key( 2800 ), true );

        // THEN
        assertEquals( countBefore, propertyKeyCount( db ) );
        db.shutdown();
    }
    
    @Test
    public void concurrently_creating_same_property_key_in_different_transactions_should_end_up_with_same_key_id() throws Exception
    {
        // GIVEN
        GraphDatabaseAPI db = (GraphDatabaseAPI)new TestGraphDatabaseFactory().newImpermanentDatabase();
        OtherThreadExecutor<WorkerState> worker1 = new OtherThreadExecutor<WorkerState>( "w1", new WorkerState( db ) );
        OtherThreadExecutor<WorkerState> worker2 = new OtherThreadExecutor<WorkerState>( "w2", new WorkerState( db ) );
        worker1.execute( new BeginTx() );
        worker2.execute( new BeginTx() );

        // WHEN
        String key = "mykey";
        worker1.execute( new CreateNodeAndSetProperty( key ) );
        worker2.execute( new CreateNodeAndSetProperty( key ) );
        worker1.execute( new FinishTx() );
        worker2.execute( new FinishTx() );
        worker1.shutdown();
        worker2.shutdown();

        // THEN
        assertEquals( 1, propertyKeyCount( db ) );
        db.shutdown();
    }
    
    private final File storeDir = TargetDirectory.forTest( getClass() ).graphDbDir( true );
    
    private GraphDatabaseAPI database()
    {
        return new EmbeddedGraphDatabase( storeDir.getAbsolutePath() );
    }

    private GraphDatabaseAPI databaseWithManyPropertyKeys( int propertyKeyCount )
    {
        GraphDatabaseAPI db = database();
        Transaction tx = db.beginTx();
        try
        {
            Node node = db.createNode();
            for ( int i = 0; i < propertyKeyCount; i++ )
                node.setProperty( key( i ), true );
            tx.success();
            return db;
        }
        finally
        {
            tx.finish();
        }
    }
    
    private String key( int i )
    {
        return "key" + i;
    }

    private Node createNodeWithProperty( GraphDatabaseService db, String key, Object value )
    {
        Transaction tx = db.beginTx();
        try
        {
            Node node = db.createNode();
            node.setProperty( key, value );
            tx.success();
            return node;
        }
        finally
        {
            tx.finish();
        }
    }

    private int propertyKeyCount( GraphDatabaseAPI db )
    {
        return (int) db.getXaDataSourceManager().getNeoStoreDataSource().getNeoStore().getPropertyStore().getPropertyKeyTokenStore().getHighId();
    }
    
    private static class WorkerState
    {
        protected final GraphDatabaseService db;
        protected Transaction tx;
        
        WorkerState( GraphDatabaseService db )
        {
            this.db = db;
        }
    }
    
    private static class BeginTx implements WorkerCommand<WorkerState, Void>
    {
        @Override
        public Void doWork( WorkerState state )
        {
            state.tx = state.db.beginTx();
            return null;
        }
    }
    
    private static class CreateNodeAndSetProperty implements WorkerCommand<WorkerState, Void>
    {
        private final String key;

        public CreateNodeAndSetProperty( String key )
        {
            this.key = key;
        }
        
        @Override
        public Void doWork( WorkerState state )
        {
            Node node = state.db.createNode();
            node.setProperty( key, true );
            return null;
        }
    }
    
    private static class FinishTx implements WorkerCommand<WorkerState, Void>
    {
        @Override
        public Void doWork( WorkerState state )
        {
            state.tx.success();
            state.tx.finish();
            return null;
        }
    }
}

/**
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
package org.neo4j.kernel.impl.transaction;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.test.OtherThreadExecutor;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static org.junit.Assert.fail;

public class TestManualAcquireLock extends AbstractNeo4jTestCase
{
    private Worker worker;
    
    @Before
    public void doBefore() throws Exception
    {
        worker = new Worker();
    }

    @After
    public void doAfter() throws Exception
    {
        worker.close();
    }
    
    @Test
    public void releaseReleaseManually() throws Exception
    {
        String key = "name";
        Node node = getGraphDb().createNode();
        
        Transaction tx = newTransaction();
        Worker worker = new Worker();
        Lock nodeLock = tx.acquireWriteLock( node );
        worker.beginTx();
        try
        {
            worker.setProperty( node, key, "ksjd" );
            fail( "Shouldn't be able to grab it" );
        }
        catch ( Exception e )
        {
        }
        nodeLock.release();
        worker.setProperty( node, key, "yo" );
        worker.finishTx();
    }
    
    @Test
    public void canOnlyReleaseOnce() throws Exception
    {
        Node node = getGraphDb().createNode();
        
        Transaction tx = newTransaction();
        Lock nodeLock = tx.acquireWriteLock( node );
        nodeLock.release();
        try
        {
            nodeLock.release();
            fail( "Shouldn't be able to release more than once" );
        }
        catch ( IllegalStateException e )
        { // Good
        }
    }
    
    @Test
    public void makeSureNodeStaysLockedEvenAfterManualRelease() throws Exception
    {
        String key = "name";
        Node node = getGraphDb().createNode();
        
        Transaction tx = newTransaction();
        Lock nodeLock = tx.acquireWriteLock( node );
        node.setProperty( key, "value" );
        nodeLock.release();
        
        Worker worker = new Worker();
        worker.beginTx();
        try
        {
            worker.setProperty( node, key, "ksjd" );
            fail( "Shouldn't be able to grab it" );
        }
        catch ( Exception e )
        {
        }
        commit();
        tx.success();
        tx.finish();
        worker.finishTx();
    }
    
    private class State
    {
        private final GraphDatabaseService graphDb;
        private Transaction tx;
        
        public State( GraphDatabaseService graphDb )
        {
            this.graphDb = graphDb;
        }
    }
    
    private class Worker extends OtherThreadExecutor<State>
    {
        public Worker()
        {
            super( "other thread", new State( getGraphDb() ) );
        }
        
        void beginTx() throws Exception
        {
            execute( new WorkerCommand<State, Void>()
            {
                @Override
                public Void doWork( State state )
                {
                    state.tx = state.graphDb.beginTx();
                    return null;
                }
            } );
        }
        
        void finishTx() throws Exception
        {
            execute( new WorkerCommand<State, Void>()
            {
                @Override
                public Void doWork( State state )
                {
                    state.tx.success();
                    state.tx.finish();
                    return null;
                }
            } );
        }
        
        void setProperty( final Node node, final String key, final Object value ) throws Exception
        {
            execute( new WorkerCommand<State, Object>()
            {
                @Override
                public Object doWork( State state )
                {
                    node.setProperty( key, value );
                    return null;
                }
            }, 200, MILLISECONDS );
        }
    }
}

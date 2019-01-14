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
package org.neo4j.kernel.impl.transaction;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.concurrent.ExecutionException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.GraphTransactionRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.fail;

public class ManualAcquireLockTest
{
    public final DatabaseRule db = new ImpermanentDatabaseRule();
    public final GraphTransactionRule tx = new GraphTransactionRule( db );

    @Rule
    public final TestRule chain = RuleChain.outerRule( db ).around( tx );

    private Worker worker;

    @Before
    public void doBefore()
    {
        worker = new Worker();
    }

    @After
    public void doAfter()
    {
        worker.close();
    }

    @Test
    public void releaseReleaseManually() throws Exception
    {
        String key = "name";
        Node node = getGraphDb().createNode();

        tx.success();
        Transaction current = tx.begin();
        Lock nodeLock = current.acquireWriteLock( node );
        worker.beginTx();
        try
        {
            worker.setProperty( node, key, "ksjd" );
            fail( "Shouldn't be able to grab it" );
        }
        catch ( Exception ignored )
        {
        }
        nodeLock.release();
        worker.setProperty( node, key, "yo" );

        try
        {
            worker.finishTx();
        }
        catch ( ExecutionException e )
        {
            // Ok, interrupting the thread while it's waiting for a lock will lead to tx failure.
        }
    }

    @Test
    public void canOnlyReleaseOnce()
    {
        Node node = getGraphDb().createNode();

        tx.success();
        Transaction current = tx.begin();
        Lock nodeLock = current.acquireWriteLock( node );
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

        tx.success();
        Transaction current = tx.begin();
        Lock nodeLock = current.acquireWriteLock( node );
        node.setProperty( key, "value" );
        nodeLock.release();

        worker.beginTx();
        try
        {
            worker.setProperty( node, key, "ksjd" );
            fail( "Shouldn't be able to grab it" );
        }
        catch ( Exception ignored )
        {
        }

        tx.success();

        try
        {
            worker.finishTx();
        }
        catch ( ExecutionException e )
        {
            // Ok, interrupting the thread while it's waiting for a lock will lead to tx failure.
        }
    }

    private GraphDatabaseService getGraphDb()
    {
        return db.getGraphDatabaseAPI();
    }

    private class State
    {
        private final GraphDatabaseService graphDb;
        private Transaction tx;

        State( GraphDatabaseService graphDb )
        {
            this.graphDb = graphDb;
        }
    }

    private class Worker extends OtherThreadExecutor<State>
    {
        Worker()
        {
            super( "other thread", new State( getGraphDb() ) );
        }

        void beginTx() throws Exception
        {
            execute( (WorkerCommand<State,Void>) state ->
            {
                state.tx = state.graphDb.beginTx();
                return null;
            } );
        }

        void finishTx() throws Exception
        {
            execute( (WorkerCommand<State,Void>) state ->
            {
                state.tx.success();
                state.tx.close();
                return null;
            } );
        }

        void setProperty( final Node node, final String key, final Object value ) throws Exception
        {
            execute( state ->
            {
                node.setProperty( key, value );
                return null;
            }, 2, SECONDS );
        }
    }
}

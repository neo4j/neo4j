/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.index.impl.lucene;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.index.Neo4jTestCase;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import java.io.File;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.neo4j.index.Neo4jTestCase.assertCollection;


public class TestIndexDeletion
{
    private static final String INDEX_NAME = "index";
    private static GraphDatabaseService graphDb;
    private static LuceneIndexProvider provider;
    private Index<Node> index;
    private Transaction tx;
    private String key;
    private Node node;
    private String value;

    @BeforeClass
    public static void setUpStuff()
    {
        String storeDir = "target/var/freshindex";
        Neo4jTestCase.deleteFileOrDirectory( new File( storeDir ) );
        graphDb = new EmbeddedGraphDatabase( storeDir );
        provider = new LuceneIndexProvider( graphDb );
    }

    @AfterClass
    public static void tearDownStuff()
    {
        graphDb.shutdown();
    }

    @After
    public void commitTx()
    {
        finishTx( true );
    }

    public void rollbackTx()
    {
        finishTx( false );
    }

    public void finishTx( boolean success )
    {
        if ( tx != null )
        {
            if ( success )
            {
                tx.success();
            }
            tx.finish();
            tx = null;
        }
    }

    @Before
    public void createInitialData()
    {
        beginTx();
        index = provider.nodeIndex( INDEX_NAME, LuceneIndexProvider.EXACT_CONFIG );
        key = "key";
        value = "my own value";
        node = graphDb.createNode();
        index.add( node, key, value );
    }

    public void beginTx()
    {
        if ( tx == null )
        {
            tx = graphDb.beginTx();
        }
    }

    void restartTx()
    {
        finishTx( true );
        beginTx();
    }

    @Test
    public void shouldBeAbleToDeleteAndRecreateIndex()
    {
        restartTx();
        assertCollection( index.query( key, "own" ) );
        index.delete();
        restartTx();

        Index<Node> recreatedIndex = provider.nodeIndex( INDEX_NAME, LuceneIndexProvider.FULLTEXT_CONFIG );
        assertNull( recreatedIndex.get( key, value ).getSingle() );
        recreatedIndex.add( node, key, value );
        restartTx();
        assertCollection( recreatedIndex.query( key, "own" ), node );
        recreatedIndex.delete();
    }

    @Test
    public void shouldNotBeDeletedWhenDeletionRolledBack()
    {
        restartTx();
        index.delete();
        rollbackTx();
        index.get( key, value );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldThrowIllegalStateForActionsAfterDeletedOnIndex()
    {
        restartTx();
        index.delete();
        restartTx();
        index.query( key, "own" );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldThrowIllegalStateForActionsAfterDeletedOnIndex2()
    {
        restartTx();
        index.delete();
        restartTx();
        index.add( node, key, value );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldThrowIllegalStateForActionsAfterDeletedOnIndex3()
    {
        restartTx();
        index.delete();
        index.query( key, "own" );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldThrowIllegalStateForActionsAfterDeletedOnIndex4()
    {
        restartTx();
        index.delete();
        Index<Node> newIndex = provider.nodeIndex( INDEX_NAME, LuceneIndexProvider.EXACT_CONFIG );
        newIndex.query( key, "own" );
    }

    @Test
    public void deleteInOneTxShouldNotAffectTheOther() throws InterruptedException
    {
        index.delete();
        OtherTransaction other = new OtherTransaction();
        other.go();
    }

    @Test
    public void deleteAndCommitShouldBePublishedToOtherTransaction() throws InterruptedException
    {
        commitTx();
        FirstTransaction firstTx = new FirstTransaction();
        SecondTransaction secondTx = new SecondTransaction();

        firstTx.doFirstStep();
        secondTx.doFirstStep();

        firstTx.doSecondStep();
        secondTx.doSecondStep();

        assertNotNull( secondTx.exception );
    }

    private class OtherTransaction extends Thread
    {
        public void go() throws InterruptedException
        {
            start();
            join();
        }

        @Override
        public void run()
        {
            Transaction tx = graphDb.beginTx();
            try
            {
                Node node = graphDb.createNode();
                index.add( node, key, "another value" );
                tx.success();
            }
            finally
            {
                tx.finish();
            }
        }
    }

    private abstract class TxThread extends Thread
    {
        private final CountDownLatch latch = new CountDownLatch( 1 );

        private void waitFor( Thread tx ) throws InterruptedException
        {
            while ( tx.getState() != State.WAITING )
            {
                Thread.sleep( 10 );
            }
        }

        public void doFirstStep() throws InterruptedException
        {
            start();
            waitFor( this );
        }

        public void doSecondStep() throws InterruptedException
        {
            this.latch.countDown();
            join();
        }

        @Override
        public void run()
        {
            Transaction tx = graphDb.beginTx();
            try
            {
                doFirst();
                latch.await();
                doSecond();
                tx.success();
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
            finally
            {
                tx.finish();
            }
        }

        protected abstract void doSecond();

        protected abstract void doFirst();
    }

    private class FirstTransaction extends TxThread
    {
        @Override
        protected void doFirst()
        {
            index.add( graphDb.createNode(), key, "another value" );
        }

        @Override
        protected void doSecond()
        {
            index.delete();
        }
    }

    private class SecondTransaction extends TxThread
    {
        private IllegalStateException exception;

        @Override
        protected void doFirst()
        {
            index.add( graphDb.createNode(), key, "my own value" );
        }

        @Override
        protected void doSecond()
        {
            try
            {
                index.get( key, "something" );
            }
            catch ( IllegalStateException e )
            {
                this.exception = e;
            }
        }
    }
}

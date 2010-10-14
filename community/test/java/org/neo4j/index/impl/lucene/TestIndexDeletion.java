package org.neo4j.index.impl.lucene;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.neo4j.index.Neo4jTestCase.assertCollection;

import java.io.File;
import java.lang.Thread.State;
import java.util.concurrent.CountDownLatch;

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
        other.start();
        other.join();
    }
    
    @Test
    public void deleteAndCommitShouldBePublishedToOtherTransaction() throws InterruptedException
    {
        commitTx();
        FirstTransaction firstTx = new FirstTransaction();
        SecondTransaction secondTx = new SecondTransaction();
        firstTx.start();
        secondTx.start();
        waitFor( firstTx, State.WAITING );
        waitFor( secondTx, State.WAITING );
        firstTx.countDown();
        firstTx.join();
        secondTx.countDown();
        secondTx.join();
        assertNotNull( secondTx.exception );
    }
    
    private void waitFor( Thread tx, State waiting ) throws InterruptedException
    {
        while ( tx.getState() != State.WAITING )
        {
            Thread.sleep( 10 );
        }
    }

    private class OtherTransaction extends Thread
    {
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
        
        protected void countDown()
        {
            this.latch.countDown();
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

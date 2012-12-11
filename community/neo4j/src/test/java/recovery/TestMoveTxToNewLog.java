/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package recovery;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import javax.transaction.xa.Xid;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.TxManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.kernel.impl.transaction.xaframework.XaResourceManager;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.test.AbstractSubProcessTestBase;
import org.neo4j.test.subprocess.BreakPoint;
import org.neo4j.test.subprocess.DebugInterface;
import org.neo4j.test.subprocess.DebuggedThread;
import org.neo4j.test.subprocess.KillSubProcess;

import static org.junit.Assert.*;
import static org.neo4j.helpers.Exceptions.*;

/**
 * Test for an issue where transactions without DONE (although with COMMIT) records
 * would be transferred to the new log after a rotation. The problem here arises in this scenario:
 * 
 * - Transactions A,B are executed and committed (in that order) in different threads.
 * - B writes the DONE record, whereas the thread which commits A doesn't make it before a rotation is performed,
 *   or fails during commit (setting TM in not-OK state).
 * - A is moved to the new log in the rotation process (since it's lacking the DONE record).
 * - non-clean shutdown occurs and the next startup will do recovery and apply A again (but not B).
 * 
 * There... if A removes ids that gets reused in B then A will overwrite parts of B when it
 * gets applied in the recovery phase. The problem is that A is allowed to be transferred to the
 * new log whereas B (which originally was executed after A) isn't. So A,B was applied (in that order),
 * but now we have a state where B,A (in that order) have been applied.
 * 
 * @author Mattias Persson
 */
public class TestMoveTxToNewLog extends AbstractSubProcessTestBase
{
    private static final String NAME = "name";
    private static final String LONG_STRING_1 = "A long string property which makes it use a dynamic record. Is it long enough you think?";
    private static final String LONG_STRING_2 = "Another long string which surpasses the comfy boundaries of a property record, shall we make it slightly longer?";
    private static final Class<?> TRANSACTION_IMPL_CLASS = safeClassForName( "org.neo4j.kernel.impl.transaction.TransactionImpl" );
    
    /* The first transaction starts committing and fails (due to the ill behaving data source)
     * The other transaction will wait for this transaction to fail (just before setting TM not OK */
    private final CountDownLatch waitForFailure = new CountDownLatch( 1 );
    
    /* The other transaction will commit successfully and trigger this after it's done */
    private final CountDownLatch waitForDone = new CountDownLatch( 2 );
    
    private DebuggedThread failingCommitThread;
    
    private final BreakPoint setTmNotOk = new BreakPoint( TxManager.class, "setTmNotOk", Throwable.class )
    {
        @Override
        protected void callback( DebugInterface debug ) throws KillSubProcess
        {
            failingCommitThread = debug.thread().suspend( this );
            waitForFailure.countDown();
        }
    };
    private final BreakPoint beforeCommit = new BreakPoint( TxManager.class, "commit", Thread.class, TRANSACTION_IMPL_CLASS )
    {
        @Override
        protected void callback( DebugInterface debug ) throws KillSubProcess
        {
            failingCommitThread.resume();
        }
    };
    private final BreakPoint failingCommitDone = new BreakPoint( getClass(), "failingCommitDone" )
    {
        @Override
        protected void callback( DebugInterface debug ) throws KillSubProcess
        {
            waitForDone.countDown();
        }
    };
    private final BreakPoint successfulCommitDone = new BreakPoint( getClass(), "successfulCommitDone" )
    {
        @Override
        protected void callback( DebugInterface debug ) throws KillSubProcess
        {
            waitForDone.countDown();
        }
    };
    
    /* Is run before the real test is performed. Sets 'name' property on reference node. */
    private static class SetPropertyTask implements Task
    {
        @Override
        public void run( GraphDatabaseAPI graphdb )
        {
            Transaction tx = graphdb.beginTx();
            try
            {
                graphdb.getReferenceNode().setProperty( NAME, LONG_STRING_1 );
                tx.success();
            }
            finally
            {
                tx.finish();
            }
        }
    }
    
    /* Removes the property (set with SetPropertyTask) from the reference node and fiddles
     * with the WriteTransaction state so that it will fail during commit */
    private static class RemovePropertyAndFailTask implements Task
    {
        @Override
        public void run( GraphDatabaseAPI graphdb )
        {
            Transaction tx = graphdb.beginTx();
            try
            {
                graphdb.getReferenceNode().removeProperty( NAME );
                messUpInternalWriteTransactionStateSoThatCommitFails( graphdb );
                tx.success();
            }
            finally
            {
                try
                {
                    tx.finish();
                }
                finally
                {
                    failingCommitDone();
                }
            }
        }
    }
    
    /* Creates a new node with a 'name' property which requires a dynamic string record.
     * The record removed by RemovePropertyAndFailTask will be reused here (that's the intention at least) */
    private static class CreateNamedNodeTask implements Task
    {
        @Override
        public void run( GraphDatabaseAPI graphdb )
        {
            Transaction tx = graphdb.beginTx();
            try
            {
                graphdb.createNode().setProperty( NAME, LONG_STRING_2 );
                tx.success();
            }
            finally
            {
                tx.finish();
                successfulCommitDone();
            }
        }
    }
    
    /* Rotates the logical log of neostore data source. This will move transaction A over
     * (since it's lacking the DONE record) */
    private static class RotateTask implements Task
    {
        @Override
        public void run( GraphDatabaseAPI graphdb )
        {
            try
            {
                graphdb.getXaDataSourceManager().getXaDataSource( Config.DEFAULT_DATA_SOURCE_NAME ).rotateLogicalLog();
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
    }
    
    /* Verifies the number of active transactions in XaLogicalLog */
    private static class VerifyActiveTransactions implements Task
    {
        private final int count;

        public VerifyActiveTransactions( int count )
        {
            this.count = count;
        }

        @SuppressWarnings( "rawtypes" )
        @Override
        public void run( GraphDatabaseAPI graphdb )
        {
            try
            {
                XaLogicalLog log = graphdb.getXaDataSourceManager().getXaDataSource(
                        Config.DEFAULT_DATA_SOURCE_NAME ).getXaContainer().getLogicalLog();
                ArrayMap activeXids = (ArrayMap) inaccessibleField( log, "xidIdentMap" ).get( log );
                assertEquals( count, activeXids.size() );
            }
            catch ( Exception e )
            {
                throw launderedException( e );
            }
        }
    }
    
    /* Verifies that the data is in a correct state despite the steps performed in this test
     * to try to break data consistency. */
    private static class VerifyTask implements Task
    {
        @Override
        public void run( GraphDatabaseAPI graphdb )
        {
            assertFalse( graphdb.getReferenceNode().hasProperty( NAME ) );
            assertEquals( LONG_STRING_2, graphdb.getNodeById( 1 ).getProperty( NAME ) );
        }
    }
    
    @SuppressWarnings( "unchecked" )
    private static void messUpInternalWriteTransactionStateSoThatCommitFails( GraphDatabaseAPI graphdb )
    {
        /* Behold: the path to WriteTransaction:
         * 
         * graphdb -> txManager -> xaDSManager -> neoStoreXaDS -> xaContainer -> xaResourceManager -> xidMap ->
         * [first element, a XidStatus] -> txStatus -> xaTransaction
         * 
         * This assumes that the transaction you're messing up is the first one started of any active transactions.
         * After a call to this method WriteTransaction#doCommit() will fail with NullPointerException
         * after all commands have been applied and before NeoStore#setLastCommittedTx() */
        try
        {
            XaResourceManager resourceManager = graphdb.getXaDataSourceManager().getXaDataSource(
                    Config.DEFAULT_DATA_SOURCE_NAME ).getXaContainer().getResourceManager();
            ArrayMap<Xid, ?> xidMap = (ArrayMap<Xid, ?>) inaccessibleField( resourceManager, "xidMap" ).get( resourceManager );
            Object xidStatus = xidMap.values().iterator().next();
            Object txStatus = inaccessibleField( xidStatus, "txStatus" ).get( xidStatus );
            Object writeTransaction = inaccessibleField( txStatus, "xaTransaction" ).get( txStatus );
            
            /* The beauty of setting lockReleaser to null is that it isn't used in doPrepare()
             * but only in doCommit() and after all commands have been applied (and ids freed). */
            inaccessibleField( writeTransaction, "lockReleaser" ).set( writeTransaction, null );
        }
        catch ( Exception e )
        {
            throw new Error( e );
        }
    }
    
    private static Class<?> safeClassForName( String className )
    {
        try
        {
            return Class.forName( className );
        }
        catch ( ClassNotFoundException e )
        {
            throw new Error( e );
        }
    }

    static void failingCommitDone()
    {   // Activates break point with the same name
    }
    
    static void successfulCommitDone()
    {   // Activates break point with the same name
    }

    @Override
    protected BreakPoint[] breakpoints( int id )
    {
        return new BreakPoint[] { setTmNotOk.enable(), beforeCommit/*.enable() later*/,
                failingCommitDone.enable(), successfulCommitDone.enable() };
    }

    @Test
    public void executeTx_A_Then_B_Where_A_FailsBeforeDoneRecordThenRotateAndRecover() throws Exception
    {
        /* Create the initial state (a long string property on any (in this case the reference) node). */
        run( new SetPropertyTask() );
        
        /* Transaction A (will fail in WriteTransaction#doCommit() and suspend right before setting TM not OK) */
        runInThread( new RemovePropertyAndFailTask() );
        waitForFailure.await();
        beforeCommit.enable();
        
        /* Transaction B (will execute normally and after that resume tx A to complete the failure) */
        run( new CreateNamedNodeTask() );
        waitForDone.await();
        
        run( new VerifyActiveTransactions( 1 ) );
        
        /* Here we should have ended up with an active nioneo logical log that contains tx A (w/o DONE record)
         * and then B (w/ a DONE record). Verify that somehow?
         * Now rotate the logical log (this should make A and everything after that to be copied over to new log) */
        run( new RotateTask() );
        
        run( new VerifyActiveTransactions( 1 ) );
        
        /* Restart the db. Here a recovery will take place and should apply transaction A in
         * that process so that the relationship created in transaction B is lost. */
        restart();
        
        /* Verify so that there are two relationships, A and B */
        run( new VerifyTask() );
    }
}

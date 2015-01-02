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
package recovery;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import javax.transaction.xa.Xid;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.helpers.UTF8;
import org.neo4j.index.impl.lucene.LuceneDataSource;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.impl.transaction.TxLog;
import org.neo4j.kernel.impl.transaction.xaframework.ForceMode;
import org.neo4j.kernel.impl.transaction.xaframework.XaResourceHelpImpl;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.AbstractSubProcessTestBase;
import org.neo4j.test.subprocess.BreakPoint;
import org.neo4j.test.subprocess.DebugInterface;
import org.neo4j.test.subprocess.DebuggedThread;
import org.neo4j.test.subprocess.KillSubProcess;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

// TODO These tests need review. Don't work after refactoring

@SuppressWarnings( "serial" )
public class TestRecoveryIssues extends AbstractSubProcessTestBase
{
    private static final byte[] NEOKERNL = { 'N', 'E', 'O', 'K', 'E', 'R', 'N', 'L', '\0' };
    private final CountDownLatch afterWrite = new CountDownLatch( 1 ), afterCrash = new CountDownLatch( 1 );

    //@Test
    public void canRecoverPreparedTransactionByDirectionFromTxManagerAfterCrashInCommit() throws Exception
    {
        for ( BreakPoint bp : breakpoints )
            bp.enable();
        runInThread( new TwoWriteTransactions() );
        afterWrite.await();
        startSubprocesses();
        run( new Verification() );
    }

    //@Test
    public void canRecoverPreparedTransactionByDirectionFromTxManagerIfItIsTheOnlyTransactionInTheLogicalLog() throws Exception
    {
        for ( BreakPoint bp : breakpoints )
            bp.enable();
        runInThread( new SingleWriteTransaction() );
        afterWrite.await();
        startSubprocesses();
        run( new Verification() );
    }

    //@Test
    public void canRecoverPreparedTransactionByDirectionFromTxManagerIfCrashingTwice() throws Exception
    {
        stopSubprocesses();
        startSubprocesses();
        for ( BreakPoint bp : breakpoints )
            bp.enable();
        runInThread( new TwoWriteTransactions() );
        afterWrite.await();
        startSubprocesses();
        runInThread( new Crash() );
        afterCrash.await();
        startSubprocesses();
        run( new Verification() );
    }

    static class TwoWriteTransactions implements Task
    {
        @Override
        public void run( GraphDatabaseAPI graphdb )
        {
            Node node;
            try(Transaction tx = graphdb.beginTx())
            {
                node = graphdb.createNode();

                tx.success();
            }
            try(Transaction tx = graphdb.beginTx())
            {
                node.setProperty( "correct", "yes" );
                graphdb.index().forNodes( "nodes" ).add( node, "name", "value" );

                tx.success();
            }
        }
    }

    static class SingleWriteTransaction implements Task
    {
        @Override
        public void run( GraphDatabaseAPI graphdb )
        {
            try(Transaction tx = graphdb.beginTx())
            {
                Node node = graphdb.createNode();
                node.setProperty( "correct", "yes" );
                graphdb.index().forNodes( "nodes" ).add( node, "name", "value" );

                tx.success();
            }
        }
    }

    static class Crash implements Task
    {
        @Override
        public void run( GraphDatabaseAPI graphdb )
        {
            throw new AssertionError( "Should not reach here - the breakpoint should avoid it" );
        }
    }

    static class Verification implements Task
    {
        @Override
        public void run( GraphDatabaseAPI graphdb )
        {
            assertNotNull( "No graph database", graphdb );
            Index<Node> index = graphdb.index().forNodes( "nodes" );
            assertNotNull( "No index", index );
            Node node = index.get( "name", "value" ).getSingle();
            assertNotNull( "could not get the node", node );
            assertEquals( "yes", node.getProperty( "correct" ) );
        }
    }

    private final BreakPoint[] breakpoints = new BreakPoint[] {
            new BreakPoint( XaResourceHelpImpl.class, "commit", Xid.class, boolean.class )
            {
                @Override
                protected void callback( DebugInterface debug ) throws KillSubProcess
                {
                    if ( twoPhaseCommitIn( debug.thread() ) )
                    {
                        debug.thread().suspend( null );
                        this.disable();
                        afterWrite.countDown();
                        throw KillSubProcess.withExitCode( -1 );
                    }
                }

                private boolean twoPhaseCommitIn( DebuggedThread thread )
                {
                    return !Boolean.parseBoolean( thread.getLocal( 1, "onePhase" ) );
                }
            }, new BreakPoint( Crash.class, "run", InternalAbstractGraphDatabase.class )
            {
                @Override
                protected void callback( DebugInterface debug ) throws KillSubProcess
                {
                    afterCrash.countDown();
                    throw KillSubProcess.withExitCode( -1 );
                }
            }, };

    @Override
    protected BreakPoint[] breakpoints( int id )
    {
        return breakpoints;
    }

    private final Bootstrapper bootstrap = getBootstrapperInstance( this );

    @Override
    protected Bootstrapper bootstrap( int id ) throws IOException
    {
        return bootstrap;
    }

    private static Bootstrapper getBootstrapperInstance( TestRecoveryIssues test )
    {
        try
        {
            return new Bootstrapper( test, 0 )
            {
                @Override
                protected void shutdown( GraphDatabaseService graphdb, boolean normal )
                {
                    if ( normal ) super.shutdown( graphdb, normal );
                };
            };
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    /**
     * Create a log file that fixes a store that has been subject to this issue.
     *
     * Parameters: [filename] [globalId.time] [globalId.sequence]
     *
     * Example: TestDoubleRecovery tm_tx_log.1 661819753510181175 3826
     */
    public static void main( String... args ) throws Exception
    {
        TxLog log = new TxLog( new File(args[0]), new DefaultFileSystemAbstraction(), new Monitors() );
        byte globalId[] = new byte[NEOKERNL.length + 16];
        System.arraycopy( NEOKERNL, 0, globalId, 0, NEOKERNL.length );
        ByteBuffer byteBuf = ByteBuffer.wrap( globalId );
        byteBuf.position( NEOKERNL.length );
        byteBuf.putLong( Long.parseLong( args[1] ) ).putLong( Long.parseLong( args[2] ) );
        log.txStart( globalId );
        log.addBranch( globalId, UTF8.encode( "414141" ) );
        log.addBranch( globalId, LuceneDataSource.DEFAULT_BRANCH_ID );
        log.markAsCommitting( globalId, ForceMode.unforced );
        log.force();
        log.close();
    }
}

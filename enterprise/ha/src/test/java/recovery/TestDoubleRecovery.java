/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

package recovery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import javax.transaction.xa.Xid;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.backup.OnlineBackup;
import org.neo4j.backup.OnlineBackupSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.graphdb.index.Index;
import org.neo4j.helpers.UTF8;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.impl.lucene.LuceneDataSource;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.transaction.TxLog;
import org.neo4j.kernel.impl.transaction.xaframework.ForceMode;
import org.neo4j.kernel.impl.transaction.xaframework.XaResourceHelpImpl;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.AbstractSubProcessTestBase;
import org.neo4j.test.subprocess.BreakPoint;
import org.neo4j.test.subprocess.DebugInterface;
import org.neo4j.test.subprocess.DebuggedThread;
import org.neo4j.test.subprocess.KillSubProcess;

@SuppressWarnings( "serial" )
public class TestDoubleRecovery extends AbstractSubProcessTestBase
{
    private static final byte[] NEOKERNL = { 'N', 'E', 'O', 'K', 'E', 'R', 'N', 'L', '\0' };
    private final CountDownLatch afterWrite = new CountDownLatch( 1 ), afterCrash = new CountDownLatch( 1 );

    /*
     * 1) Do a 2PC transaction, crash when both resource have been prepared and txlog
     *    says "mark as committing" for that tx.
     * 2) Do recovery and then crash again.
     * 3) Do recovery and see so that all data is in there.
     * Also do an incremental backup just to make sure that the logs have gotten the
     * right records injected.
     */
    @Ignore( "TODO Broken since the assembly merge. Please fix" )
    @Test
    public void crashAfter2PCMarkAsCommittingThenCrashAgainAndRecover() throws Exception
    {
        String backupDirectory = "target/var/backup-db";
        FileUtils.deleteRecursively( new File( backupDirectory ) );
        stopSubprocesses();
        startSubprocesses();
        OnlineBackup.from( "localhost" ).full( backupDirectory );
        for ( BreakPoint bp : breakpoints( 0 ) )
            bp.enable();
        runInThread( new WriteTransaction() );
        afterWrite.await();
        startSubprocesses();
        runInThread( new Crash() );
        afterCrash.await();
        startSubprocesses();
        OnlineBackup.from( "localhost" ).incremental( backupDirectory );
        run( new Verification() );

        EmbeddedGraphDatabase db = new EmbeddedGraphDatabase( backupDirectory );
        try
        {
            new Verification().run( db );
        }
        finally
        {
            db.shutdown();
        }
    }

    static class WriteTransaction implements Task
    {
        @Override
        public void run( GraphDatabaseAPI graphdb )
        {
            Transaction tx = graphdb.beginTx();
            Node node;
            try
            { // hack to get around another bug
                node = graphdb.createNode();

                tx.success();
            }
            finally
            {
                tx.finish();
            }
            tx = graphdb.beginTx();
            try
            {
                node.setProperty( "correct", "yes" );
                graphdb.index().forNodes( "nodes" ).add( node, "name", "value" );

                tx.success();
            }
            finally
            {
                tx.finish();
            }
        }
    }

    static class Write1PCTransaction implements Task
    {
        @Override
        public void run( GraphDatabaseAPI graphdb )
        {
            Transaction tx = graphdb.beginTx();
            Node node;
            try
            { // hack to get around another bug
                node = graphdb.createNode();

                tx.success();
            }
            finally
            {
                tx.finish();
            }
            tx = graphdb.beginTx();
            try
            {
                node.setProperty( "correct", "yes" );
                tx.success();
            }
            finally
            {
                tx.finish();
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

    private final BreakPoint ON_CRASH = new BreakPoint( Crash.class, "run", InternalAbstractGraphDatabase.class )
    {
        @Override
        protected void callback( DebugInterface debug ) throws KillSubProcess
        {
            afterCrash.countDown();
            throw KillSubProcess.withExitCode( -1 );
        }
    };

    private final BreakPoint BEFORE_ANY_DATASOURCE_2PC = new BreakPoint( XaResourceHelpImpl.class, "commit", Xid.class, boolean.class )
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
    };

    private final BreakPoint BEFORE_SECOND_1PC = new BreakPoint( XaResourceHelpImpl.class, "commit", Xid.class, boolean.class )
    {
        private int counter;

        @Override
        protected void callback( DebugInterface debug ) throws KillSubProcess
        {
            if ( onePhaseCommitIn( debug.thread() ) )
            {
                if ( ++counter == 2 )
                {
                    debug.thread().suspend( null );
                    this.disable();
                    afterWrite.countDown();
                    throw KillSubProcess.withExitCode( -1 );
                }
            }
        }

        private boolean onePhaseCommitIn( DebuggedThread thread )
        {
            return Boolean.parseBoolean( thread.getLocal( 1, "onePhase" ) );
        }
    };

//    private final BreakPoint BEFORE_TXLOG_MARK_AS_COMMITTING_2PC = new BreakPoint( TxLog.class, "markAsCommitting", byte[].class )
//    {
//        @Override
//        protected void callback( DebugInterface debug ) throws KillSubProcess
//        {
//            System.out.println( "yeah" );
//            debug.thread().suspend( null );
//            this.disable();
//            afterWrite.countDown();
//            throw new KillSubProcess( -1 );
//        }
//    };

    private final BreakPoint[] breakpointsForBefore2PC = new BreakPoint[] { ON_CRASH, BEFORE_ANY_DATASOURCE_2PC };
//    private final BreakPoint[] breakpointsForMarkAsCommitting2PC = new BreakPoint[] { ON_CRASH, BEFORE_TXLOG_MARK_AS_COMMITTING_2PC };

    @Override
    protected BreakPoint[] breakpoints( int id )
    {
        return breakpointsForBefore2PC;
    }

    private final Bootstrapper bootstrap = bootstrap( this, MapUtil.stringMap( OnlineBackupSettings.online_backup_enabled.name(), GraphDatabaseSetting.TRUE ) );

    @Override
    protected Bootstrapper bootstrap( int id ) throws IOException
    {
        return bootstrap;
    }

    private static Bootstrapper bootstrap( TestDoubleRecovery test, Map<String, String> config )
    {
        try
        {
            return new Bootstrapper( test, 0, config )
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
        EmbeddedGraphDatabase graphdb = new EmbeddedGraphDatabase( "target/test-data/junk" );
        try
        {
            new WriteTransaction().run( graphdb );
        }
        finally
        {
            graphdb.shutdown();
        }

        TxLog log = new TxLog( args[0], new DefaultFileSystemAbstraction(), StringLogger.DEV_NULL );
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

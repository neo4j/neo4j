/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.Config.DEFAULT_DATA_SOURCE_NAME;
import static org.neo4j.kernel.Config.KEEP_LOGICAL_LOGS;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.transaction.xaframework.NullLogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog.LogExtractor;
import org.neo4j.test.AbstractSubProcessTestBase;
import org.neo4j.test.subprocess.BreakPoint;

/**
 * Tries to trigger log file version errors that could happen if the db was killed
 * at the exact right time, resulting in logs (or transactions in logs) gone missing.
 * 
 * @author Mattias Persson
 * @author Johan Svensson
 */
public class TestRecoveryLogTimingIssues extends AbstractSubProcessTestBase
{
    private static final DynamicRelationshipType TYPE = withName( "TYPE" );

    private final CountDownLatch breakpointNotification = new CountDownLatch( 1 );
    private final BreakPoint SET_VERSION = BreakPoint.thatCrashesTheProcess( breakpointNotification, 0,
            NeoStore.class, "setVersion", long.class );
    private final BreakPoint RELEASE_CURRENT_LOG_FILE = BreakPoint.thatCrashesTheProcess( breakpointNotification, 0,
            XaLogicalLog.class, "releaseCurrentLogFile" );
    private final BreakPoint RENAME_LOG_FILE = BreakPoint.thatCrashesTheProcess( breakpointNotification, 0,
            XaLogicalLog.class, "renameLogFileToRightVersion", String.class, long.class );
    private final BreakPoint SET_VERSION_2 = BreakPoint.thatCrashesTheProcess( breakpointNotification, 1,
            NeoStore.class, "setVersion", long.class );
    private final BreakPoint RELEASE_CURRENT_LOG_FILE_2 = BreakPoint.thatCrashesTheProcess( breakpointNotification, 1,
            XaLogicalLog.class, "releaseCurrentLogFile" );
    private final BreakPoint RENAME_LOG_FILE_2 = BreakPoint.thatCrashesTheProcess( breakpointNotification, 1,
            XaLogicalLog.class, "renameLogFileToRightVersion", String.class, long.class );
    private final BreakPoint[] breakpoints = new BreakPoint[] {
            SET_VERSION, RELEASE_CURRENT_LOG_FILE, RENAME_LOG_FILE,
            SET_VERSION_2, RELEASE_CURRENT_LOG_FILE_2, RENAME_LOG_FILE_2 };
    
    @Override
    protected BreakPoint[] breakpoints( int id )
    {
        return breakpoints;
    }
    
    private final Bootstrapper bootstrapper = killAwareBootstrapper( this, 0, stringMap( KEEP_LOGICAL_LOGS, "true" ) );
    
    @Override
    protected Bootstrapper bootstrap( int id ) throws IOException
    {
        return bootstrapper;
    }
    
    static class DoSimpleTransaction implements Task
    {
        @Override
        public void run( AbstractGraphDatabase graphdb )
        {
            Transaction tx = graphdb.beginTx();
            try
            {
                Node parent = graphdb.createNode();
                for ( int i = 0; i < 10; i++ )
                {
                    Node child = graphdb.createNode();
                    parent.createRelationshipTo( child, TYPE );
                }
                tx.success();
            }
            finally
            {
                tx.finish();
            }
        }
    }
    
    static class RotateLogs implements Task
    {
        @Override
        public void run( AbstractGraphDatabase graphdb )
        {
            try
            {
                graphdb.getConfig().getTxModule().getXaDataSourceManager().getXaDataSource(
                        DEFAULT_DATA_SOURCE_NAME ).rotateLogicalLog();
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
    }
    
    static class GetCommittedTransactions implements Task
    {
        private final long highestLogVersion;
        private final long highestTxId;

        public GetCommittedTransactions( long highestLogVersion, long highestTxId )
        {
            this.highestLogVersion = highestLogVersion;
            this.highestTxId = highestTxId;
        }
        
        @Override
        public void run( AbstractGraphDatabase graphdb )
        {
            try
            {
                XaDataSource dataSource = graphdb.getConfig().getTxModule()
                        .getXaDataSourceManager().getXaDataSource( DEFAULT_DATA_SOURCE_NAME );
                for ( long logVersion = 0; logVersion < highestLogVersion; logVersion++ )
                {
                    dataSource.getLogicalLog( logVersion );
                }
                
                LogExtractor extractor = dataSource.getLogExtractor( 2, highestTxId );
                try
                {
                    for ( long txId = 2; txId <= highestTxId; txId++ )
                    {
                        extractor.extractNext( NullLogBuffer.INSTANCE );
                    }
                }
                finally
                {
                    extractor.close();
                }
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
        }
    }

    private void crashDuringRotateAndVerify( long highestLogVersion, long highestTxId ) throws Exception
    {
        runInThread( new RotateLogs() );
        breakpointNotification.await();
        startSubprocesses();
        run( new GetCommittedTransactions( highestLogVersion, highestTxId ) );
    }
    
    @Test
    public void logsShouldContainAllTransactionsEvenIfCrashJustBeforeNeostoreSetVersion() throws Exception
    {
        breakpoints[0].enable();
        run( new DoSimpleTransaction() );
        // tx(2) is the first one, for creating the relationship type
        // tx(3) is the second one, for doing the actual transaction in DoSimpleTransaction
        crashDuringRotateAndVerify( 1, 3 );
    }

    @Test
    public void logsShouldContainAllTransactionsEvenIfCrashJustBeforeReleaseCurrentLogFile() throws Exception
    {
        breakpoints[1].enable();
        run( new DoSimpleTransaction() );
        crashDuringRotateAndVerify( 1, 3 );
    }

    @Test
    public void logsShouldContainAllTransactionsEvenIfCrashJustAfterSetActiveVersion() throws Exception
    {
        breakpoints[2].enable();
        run( new DoSimpleTransaction() );
        crashDuringRotateAndVerify( 1, 3 );
    }

    @Test
    public void logsShouldContainAllTransactionsEvenIfCrashJustBeforeNeostoreSetVersionTwLogs() throws Exception
    {
        breakpoints[3].enable();
        run( new DoSimpleTransaction() );
        run( new RotateLogs() );
        run( new DoSimpleTransaction() );
        crashDuringRotateAndVerify( 2, 4 );
    }

    @Test
    public void logsShouldContainAllTransactionsEvenIfCrashJustBeforeReleaseCurrentLogFileTwLogs() throws Exception
    {
        breakpoints[4].enable();
        run( new DoSimpleTransaction() );
        run( new RotateLogs() );
        run( new DoSimpleTransaction() );
        crashDuringRotateAndVerify( 2, 4 );
    }

    @Test
    public void logsShouldContainAllTransactionsEvenIfCrashJustAfterSetActiveVersionTwLogs() throws Exception
    {
        breakpoints[5].enable();
        run( new DoSimpleTransaction() );
        run( new RotateLogs() );
        run( new DoSimpleTransaction() );
        crashDuringRotateAndVerify( 2, 4 );
    }
}

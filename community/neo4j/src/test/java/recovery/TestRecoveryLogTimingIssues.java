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
package recovery;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Settings;
import org.neo4j.index.impl.lucene.LuceneDataSource;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.LogExtractor;
import org.neo4j.kernel.impl.transaction.xaframework.NullLogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.test.AbstractSubProcessTestBase;
import org.neo4j.test.subprocess.BreakPoint;
import org.neo4j.test.subprocess.BreakPoint.Event;

import static java.lang.Integer.parseInt;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.helpers.SillyUtils.nonNull;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

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
            XaLogicalLog.class, "renameLogFileToRightVersion", File.class, long.class );
    private final BreakPoint SET_VERSION_2 = BreakPoint.thatCrashesTheProcess( breakpointNotification, 1,
            NeoStore.class, "setVersion", long.class );
    private final BreakPoint RELEASE_CURRENT_LOG_FILE_2 = BreakPoint.thatCrashesTheProcess( breakpointNotification, 1,
            XaLogicalLog.class, "releaseCurrentLogFile" );
    private final BreakPoint RENAME_LOG_FILE_2 = BreakPoint.thatCrashesTheProcess( breakpointNotification, 1,
            XaLogicalLog.class, "renameLogFileToRightVersion", File.class, long.class );
    private final BreakPoint EXIT_RENAME_LOG_FILE_LUCENE = BreakPoint.thatCrashesTheProcess( Event.EXIT, breakpointNotification, 0,
            BreakPoint.stackTraceMustContainClass( LuceneDataSource.class ), XaLogicalLog.class, "renameLogFileToRightVersion", File.class, long.class );
    private final BreakPoint EXIT_RENAME_LOG_FILE_NIONEO = BreakPoint.thatCrashesTheProcess( Event.EXIT, breakpointNotification, 0,
            BreakPoint.stackTraceMustNotContainClass( LuceneDataSource.class ), XaLogicalLog.class, "renameLogFileToRightVersion", File.class, long.class );
    
    private final BreakPoint[] breakpoints = new BreakPoint[] {
            SET_VERSION, RELEASE_CURRENT_LOG_FILE, RENAME_LOG_FILE,
            SET_VERSION_2, RELEASE_CURRENT_LOG_FILE_2, RENAME_LOG_FILE_2, EXIT_RENAME_LOG_FILE_LUCENE, EXIT_RENAME_LOG_FILE_NIONEO };
    
    @Override
    protected BreakPoint[] breakpoints( int id )
    {
        return breakpoints;
    }
    
    private final Bootstrapper bootstrapper = killAwareBootstrapper( this, 0, stringMap(
            GraphDatabaseSettings.keep_logical_logs.name(), Settings.TRUE ) );
    
    @Override
    protected Bootstrapper bootstrap( int id ) throws IOException
    {
        return bootstrapper;
    }
    
    static class DoSimpleTransaction implements Task
    {
        @Override
        public void run( GraphDatabaseAPI graphdb )
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
    
    static class DoGraphAndIndexTransaction implements Task
    {
        @Override
        public void run( GraphDatabaseAPI graphdb )
        {
            Transaction tx = graphdb.beginTx();
            try
            {
                Node parent = graphdb.createNode();
                graphdb.index().forNodes( "index" ).add( parent, "name", "test" );
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
        private final Set<String> dataSources = new HashSet<String>();
        
        RotateLogs( String... dataSources )
        {
            this.dataSources.addAll( Arrays.asList( dataSources ) );
        }
        
        @Override
        @SuppressWarnings("deprecation")
        public void run( GraphDatabaseAPI graphdb )
        {
            try
            {
                if ( dataSources.isEmpty() )
                    graphdb.getXaDataSourceManager().getNeoStoreDataSource().rotateLogicalLog();
                else
                {
                    for ( XaDataSource ds : graphdb.getXaDataSourceManager().getAllRegisteredDataSources() )
                    {
                        if ( dataSources.contains( ds.getName() ) )
                            ds.rotateLogicalLog();
                    }
                }
            }
            catch ( IOException e )
            {
                e.printStackTrace();
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
        @SuppressWarnings("deprecation")
        public void run( GraphDatabaseAPI graphdb )
        {
            try
            {
                XaDataSource dataSource = graphdb.getXaDataSourceManager().getNeoStoreDataSource();
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
    
    static class Shutdown implements Task
    {
        @Override
        public void run( GraphDatabaseAPI graphdb )
        {
            graphdb.shutdown();
        }
    }
    
    private static class VerifyLastTxId implements Task
    {
        private long tx;
        private String dataSource;

        VerifyLastTxId( String dataSource, long tx )
        {
            this.dataSource = dataSource;
            this.tx = tx;
        }
        
        @Override
        @SuppressWarnings("deprecation")
        public void run( GraphDatabaseAPI graphdb )
        {
            assertEquals( tx, graphdb.getXaDataSourceManager().getXaDataSource( dataSource ).getLastCommittedTxId() );
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
    public void logsShouldContainAllTransactionsEvenIfCrashJustBeforeNeostoreSetVersionTwoLogs() throws Exception
    {
        breakpoints[3].enable();
        run( new DoSimpleTransaction() );
        run( new RotateLogs() );
        run( new DoSimpleTransaction() );
        crashDuringRotateAndVerify( 2, 4 );
    }

    @Test
    public void logsShouldContainAllTransactionsEvenIfCrashJustBeforeReleaseCurrentLogFileTwoLogs() throws Exception
    {
        breakpoints[4].enable();
        run( new DoSimpleTransaction() );
        run( new RotateLogs() );
        run( new DoSimpleTransaction() );
        crashDuringRotateAndVerify( 2, 4 );
    }

    @Test
    public void logsShouldContainAllTransactionsEvenIfCrashJustAfterSetActiveVersionTwoLogs() throws Exception
    {
        breakpoints[5].enable();
        run( new DoSimpleTransaction() );
        run( new RotateLogs() );
        run( new DoSimpleTransaction() );
        crashDuringRotateAndVerify( 2, 4 );
    }

    @Test
    public void nextLogVersionAfterCrashBetweenActiveSetToCleanAndRename() throws Exception
    {
        breakpoints[2].enable();
        runInThread( new Shutdown() );
        breakpointNotification.await();
        startSubprocesses();
        run( new Shutdown() );
    }

    @Test
    public void nextLogVersionAfterCrashBetweenRenameAndIncrementVersionInCloseShouldBeTheNextOne() throws Exception
    {
        breakpoints[6].enable();
        runInThread( new Shutdown() );
        breakpointNotification.await();
        startSubprocesses();
        run( new Shutdown() );
        assertLuceneLogVersionsExists( 0, 1 );
    }

    @Test
    public void nextLogVersionAfterCrashBetweenRenameAndIncrementVersionInRotateShouldBeTheNextOne() throws Exception
    {
        breakpoints[7].enable();
        run( new DoSimpleTransaction() );
        runInThread( new RotateLogs( LuceneDataSource.DEFAULT_NAME ) );
        breakpointNotification.await();
        startSubprocesses();
        run( new Shutdown() );
        assertLuceneLogVersionsExists( 0, 1 );
    }
    
    @Test
    public void lastLuceneTxAfterRecoveredCrashBetweenRenameAndIncrementVersionInCloseShouldBeCorrect() throws Exception
    {
        breakpoints[6].enable();
        run( new DoGraphAndIndexTransaction() );
        runInThread( new Shutdown() );
        breakpointNotification.await();
        startSubprocesses();
        run( new VerifyLastTxId( LuceneDataSource.DEFAULT_NAME, 3 ) );
    }
    
    @Test
    public void lastNeoTxAfterRecoveredCrashBetweenRenameAndIncrementVersionInCloseShouldBeCorrect() throws Exception
    {
        breakpoints[7].enable();
        run( new DoSimpleTransaction() );
        runInThread( new Shutdown() );
        breakpointNotification.await();
        startSubprocesses();
        run( new VerifyLastTxId( NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME, 3 ) );
    }
    
    private void assertLuceneLogVersionsExists( int... versions ) throws Exception
    {
        Set<Integer> versionSet = new HashSet<Integer>();
        for ( int version : versions )
            versionSet.add( version );
        File path = new File( getStoreDir( this, 0, false ), "index" );
        for ( File file : nonNull( path.listFiles() ) )
        {
            if ( file.getName().contains( ".log.v" ) )
            {
                int v = parseInt( file.getName().substring( file.getName().lastIndexOf( ".v" )+2 ) );
                assertTrue( "Unexpected version found " + v, versionSet.remove( v ) );
            }
        }
        assertTrue( "These versions weren't found " + versionSet, versionSet.isEmpty() );
    }
}

/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.backup;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.neo4j.com.RequestContext;
import org.neo4j.com.RequestContext.Tx;
import org.neo4j.com.Response;
import org.neo4j.com.ServerUtil;
import org.neo4j.com.ServerUtil.TxHandler;
import org.neo4j.com.StoreWriter;
import org.neo4j.com.ToFileStoreWriter;
import org.neo4j.com.TransactionStream;
import org.neo4j.com.TxExtractor;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Settings;
import org.neo4j.helpers.Triplet;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConfigParam;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils;
import org.neo4j.kernel.impl.transaction.xaframework.NoSuchLogVersionException;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.logging.Logging;

import static java.util.Collections.emptyMap;

class BackupService
{
    class BackupOutcome
    {
        private final Map<String, Long> lastCommittedTxs;

        BackupOutcome( Map<String, Long> lastCommittedTxs )
        {
            this.lastCommittedTxs = lastCommittedTxs;
        }

        public Map<String, Long> getLastCommittedTxs()
        {
            return Collections.unmodifiableMap( lastCommittedTxs );
        }
    }

    BackupOutcome doFullBackup( String sourceHostNameOrIp, int sourcePort, String targetDirectory,
                                boolean checkConsistency, Config tuningConfiguration )
    {
        if ( directoryContainsDb( targetDirectory ) )
        {
            throw new RuntimeException( targetDirectory + " already contains a database" );
        }

        BackupClient client = new BackupClient( sourceHostNameOrIp, sourcePort, new DevNullLoggingService(), null );
        client.start();
        long timestamp = System.currentTimeMillis();
        Map<String, Long> lastCommittedTxs = emptyMap();
        try
        {
            Response<Void> response = client.fullBackup( decorateWithProgressIndicator(
                    new ToFileStoreWriter( new File( targetDirectory ) ) ) );
            GraphDatabaseAPI targetDb = startTemporaryDb( targetDirectory,
                    VerificationLevel.NONE /* run full check instead */ );
            try
            {
                // First, receive all txs pending
                lastCommittedTxs = unpackResponse( response,
                        targetDb.getDependencyResolver().resolveDependency( XaDataSourceManager.class ),
                        ServerUtil.txHandlerForFullCopy() );
                // Then go over all datasources, try to extract the latest tx
                Set<String> noTxPresent = new HashSet<String>();
                for ( XaDataSource ds : targetDb.getXaDataSourceManager().getAllRegisteredDataSources() )
                {
                    long lastTx = ds.getLastCommittedTxId();
                    try
                    {
                        // This fails if the tx is not present with NSLVE
                        ds.getMasterForCommittedTx( lastTx );
                    }
                    catch ( NoSuchLogVersionException e )
                    {
                        // Note the name of the datasource
                        noTxPresent.add( ds.getName() );
                    }
                    catch ( IOException e )
                    {
                        throw new RuntimeException( e );
                    }
                }
                if ( !noTxPresent.isEmpty() )
                {
                    /*
                     * Create a fake slave context, asking for the transactions that
                     * span the next-to-last up to the latest for each datasource
                     */
                    BackupClient recoveryClient = new BackupClient(
                            sourceHostNameOrIp, sourcePort, targetDb.getDependencyResolver().resolveDependency( Logging.class ), targetDb.getStoreId() );
                    recoveryClient.start();
                    Response<Void> recoveryResponse = null;
                    Map<String, Long> recoveryDiff = new HashMap<String, Long>();
                    for ( String ds : noTxPresent )
                    {
                        recoveryDiff.put( ds, -1L );
                    }
                    RequestContext recoveryCtx = addDiffToSlaveContext(
                            slaveContextOf( targetDb ), recoveryDiff );
                    try
                    {
                        recoveryResponse = recoveryClient.incrementalBackup( recoveryCtx );
                        // Ok, the response is here, apply it.
                        TransactionStream txs = recoveryResponse.transactions();
                        ByteBuffer scratch = ByteBuffer.allocate( 64 );
                        while ( txs.hasNext() )
                        {
                            /*
                             * For each tx stream in the response, create the latest archived
                             * logical log file and write out in there the transaction.
                             *
                             */
                            Triplet<String, Long, TxExtractor> tx = txs.next();
                            scratch.clear();
                            XaDataSource ds = targetDb.getXaDataSourceManager().getXaDataSource(
                                    tx.first() );
                            long logVersion = ds.getCurrentLogVersion() - 1;
                            FileChannel newLog = new RandomAccessFile(
                                    ds.getFileName( logVersion ),
                                    "rw" ).getChannel();
                            newLog.truncate( 0 );
                            LogIoUtils.writeLogHeader( scratch, logVersion, -1 );
                            // scratch buffer is flipped by writeLogHeader
                            newLog.write( scratch );
                            ReadableByteChannel received = tx.third().extract();
                            scratch.flip();
                            while ( received.read( scratch ) > 0 )
                            {
                                scratch.flip();
                                newLog.write( scratch );
                                scratch.flip();
                            }
                            newLog.force( false );
                            newLog.close();
                            received.close();
                        }
                    }
                    catch ( IOException e )
                    {
                        throw new RuntimeException( e );
                    }
                    finally
                    {
                        try
                        {
                            recoveryClient.stop();
                        }
                        catch ( Throwable throwable )
                        {
                            throw new RuntimeException( throwable );
                        }
                        if ( recoveryResponse != null )
                        {
                            recoveryResponse.close();
                        }
                        targetDb.shutdown();
                    }
                }
            }
            finally
            {
                targetDb.shutdown();
            }
            bumpLogFile( targetDirectory, timestamp );
            if ( checkConsistency )
            {
                StringLogger logger = StringLogger.SYSTEM;
                try
                {
                    new ConsistencyCheckService().runFullConsistencyCheck(
                            targetDirectory,
                            tuningConfiguration,
                            ProgressMonitorFactory.textual( System.err ),
                            logger );
                }
                catch ( ConsistencyCheckIncompleteException e )
                {
                    e.printStackTrace( System.err );
                }
                finally
                {
                    logger.flush();
                }
            }
        }
        finally
        {
            try
            {
                client.stop();
            }
            catch ( Throwable throwable )
            {
                throw new RuntimeException( throwable );
            }
        }
        return new BackupOutcome( lastCommittedTxs );
    }

    BackupOutcome doIncrementalBackup( String sourceHostNameOrIp, int sourcePort, String targetDirectory,
                                       boolean verification )
    {
        if ( !directoryContainsDb( targetDirectory ) )
        {
            throw new RuntimeException( targetDirectory + " doesn't contain a database" );
        }

        // In case someone deleted the logical log from a full backup
        ConfigParam keepLogs = new ConfigParam()
        {
            @Override
            public void configure( Map<String, String> config )
            {
                config.put( GraphDatabaseSettings.keep_logical_logs.name(), Settings.TRUE );
            }
        };

        GraphDatabaseAPI targetDb = startTemporaryDb( targetDirectory,
                VerificationLevel.valueOf( verification ), keepLogs );

        long backupStartTime = System.currentTimeMillis();
        BackupOutcome outcome = null;
        try
        {
            outcome = doIncrementalBackup( sourceHostNameOrIp, sourcePort, targetDb );
        }
        finally
        {
            targetDb.shutdown();
        }

        bumpLogFile( targetDirectory, backupStartTime );
        return outcome;
    }

    BackupOutcome doIncrementalBackup( String sourceHostNameOrIp, int sourcePort, GraphDatabaseAPI targetDb )
    {
        return incrementalWithContext( sourceHostNameOrIp, sourcePort, targetDb, slaveContextOf( targetDb ) );
    }

    private RequestContext slaveContextOf( GraphDatabaseAPI graphDb )
    {
        XaDataSourceManager dsManager = graphDb.getXaDataSourceManager();
        List<Tx> txs = new ArrayList<Tx>();
        for ( XaDataSource ds : dsManager.getAllRegisteredDataSources() )
        {
            txs.add( RequestContext.lastAppliedTx( ds.getName(), ds.getLastCommittedTxId() ) );
        }
        return RequestContext.anonymous( txs.toArray( new Tx[txs.size()] ) );
    }

    private StoreWriter decorateWithProgressIndicator( final StoreWriter actual )
    {
        return new StoreWriter()
        {
            private final ProgressListener progress = ProgressMonitorFactory.textual( System.out ).openEnded( "Files copied", 1 );

            @Override
            public void write( String path, ReadableByteChannel data, ByteBuffer temporaryBuffer,
                               boolean hasData ) throws IOException
            {
                actual.write( path, data, temporaryBuffer, hasData );
                progress.add( 1 );
            }

            @Override
            public void done()
            {
                actual.done();
                progress.done();
            }
        };
    }

    boolean directoryContainsDb( String targetDirectory )
    {
        return new File( targetDirectory, NeoStore.DEFAULT_NAME ).exists();
    }

    static GraphDatabaseAPI startTemporaryDb( String targetDirectory, ConfigParam... params )
    {
        Map<String, String> config = new HashMap<String, String>();
        config.put( OnlineBackupSettings.online_backup_enabled.name(), Settings.FALSE );
        for ( ConfigParam param : params )
        {
            if ( param != null )
            {
                param.configure( config );
            }
        }
        return (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( targetDirectory ).setConfig( config ).newGraphDatabase();
    }

    private RequestContext addDiffToSlaveContext( RequestContext original,
                                                  Map<String, Long> diffPerDataSource )
    {
        Tx[] oldTxs = original.lastAppliedTransactions();
        Tx[] newTxs = new Tx[oldTxs.length];
        for ( int i = 0; i < oldTxs.length; i++ )
        {
            Tx oldTx = oldTxs[i];
            String dsName = oldTx.getDataSourceName();
            long originalTxId = oldTx.getTxId();
            Long diff = diffPerDataSource.get( dsName );
            if ( diff == null )
            {
                diff = 0L;
            }
            long newTxId = originalTxId + diff;
            newTxs[i] = RequestContext.lastAppliedTx( dsName, newTxId );
        }
        return RequestContext.anonymous( newTxs );
    }

    /**
     * Performs an incremental backup based off the given context. This means
     * receiving and applying selectively (i.e. irrespective of the actual state
     * of the target db) a set of transactions starting at the desired txId and
     * spanning up to the latest of the master, for every data source
     * registered.
     *
     * @param targetDb           The database that contains a previous full copy
     * @param context            The context, i.e. a mapping of data source name to txid
     *                           which will be the first in the returned stream
     * @return A backup context, ready to perform
     */
    private BackupOutcome incrementalWithContext( String sourceHostNameOrIp, int sourcePort, GraphDatabaseAPI targetDb,
                                                  RequestContext context )
    {
        BackupClient client = new BackupClient( sourceHostNameOrIp, sourcePort, targetDb.getDependencyResolver().resolveDependency( Logging.class ),
                targetDb.getStoreId() );
        client.start();
        Map<String, Long> lastCommittedTxs;
        try
        {
            lastCommittedTxs = unpackResponse( client.incrementalBackup( context ),
                    targetDb.getDependencyResolver().resolveDependency( XaDataSourceManager.class ),
                    new ProgressTxHandler() );
            trimLogicalLogCount( targetDb );
        }
        finally
        {
            try
            {
                client.stop();
            }
            catch ( Throwable throwable )
            {
                throw new RuntimeException( throwable );
            }
        }
        return new BackupOutcome( lastCommittedTxs );
    }

    private void trimLogicalLogCount( GraphDatabaseAPI targetDb )
    {
        for ( XaDataSource ds : targetDb.getXaDataSourceManager().getAllRegisteredDataSources() )
        {
            try
            {
                ds.rotateLogicalLog();
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }

            long currentVersion = ds.getCurrentLogVersion() - 1;

            // TODO
            /*
             * Checking the file size to determine if transactions exist in
             * a log feels hack-ish. Maybe fix this to read the header
             * and check latest txid?
             */
            while ( ds.getLogicalLogLength( currentVersion ) <= 16
                    && currentVersion > 0 )
            {
                currentVersion--;
            }
            /*
             * Ok, we skipped all logs that have no transactions in them. Current is the
             * one with the tx in it. Skip it.
             */
            currentVersion--;
            /*
             * Now delete the rest.
             */
            while ( ds.getLogicalLogLength( currentVersion ) > 0 )
            {
                ds.deleteLogicalLog( currentVersion );
                currentVersion--;
            }
        }
    }

    private Map<String, Long> unpackResponse( Response<Void> response, XaDataSourceManager xaDsm, TxHandler txHandler )
    {
        try
        {
            ServerUtil.applyReceivedTransactions( response, xaDsm, txHandler );
            return extractLastCommittedTxs( xaDsm );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to apply received transactions", e );
        }
    }

    private Map<String, Long> extractLastCommittedTxs( XaDataSourceManager xaDsm )
    {
        TreeMap<String, Long> lastCommittedTxs = new TreeMap<String, Long>();
        for ( XaDataSource ds : xaDsm.getAllRegisteredDataSources() )
        {
            lastCommittedTxs.put( ds.getName(), ds.getLastCommittedTxId() );
        }
        return lastCommittedTxs;
    }

    private static boolean bumpLogFile( String targetDirectory, long toTimestamp )
    {
        File dbDirectory = new File( targetDirectory );
        File[] candidates = dbDirectory.listFiles( new FilenameFilter()
        {
            @Override
            public boolean accept( File dir, String name )
            {
                /*
                 *  Contains ensures that previously timestamped files are
                 *  picked up as well
                 */
                return name.equals( StringLogger.DEFAULT_NAME );
            }
        } );
        if ( candidates.length != 1 )
        {
            return false;
        }
        // candidates has a unique member, the right one
        File previous = candidates[0];
        // Build to, from existing parent + new filename
        File to = new File( previous.getParentFile(), StringLogger.DEFAULT_NAME
                + "." + toTimestamp );
        return previous.renameTo( to );
    }

    private static class ProgressTxHandler implements TxHandler
    {
        private final ProgressListener progress = ProgressMonitorFactory.textual( System.out ).openEnded( "Transactions applied", 1000 );

        @Override
        public void accept( Triplet<String, Long, TxExtractor> tx, XaDataSource dataSource )
        {
            progress.add( 1 );
        }

        @Override
        public void done()
        {
            progress.done();
        }
    }
}

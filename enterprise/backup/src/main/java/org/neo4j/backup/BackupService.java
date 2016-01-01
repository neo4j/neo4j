/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.neo4j.com.RequestContext;
import org.neo4j.com.RequestContext.Tx;
import org.neo4j.com.Response;
import org.neo4j.com.ServerUtil;
import org.neo4j.com.ServerUtil.TxHandler;
import org.neo4j.com.TxExtractor;
import org.neo4j.com.storecopy.RemoteStoreCopier;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.CancellationRequest;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.Settings;
import org.neo4j.helpers.Triplet;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConfigParam;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.MismatchingStoreIdException;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.NoSuchLogVersionException;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;

class BackupService
{
    class BackupOutcome
    {
        private final Map<String, Long> lastCommittedTxs;
        private final boolean consistent;

        BackupOutcome( Map<String, Long> lastCommittedTxs, boolean consistent )
        {
            this.lastCommittedTxs = lastCommittedTxs;
            this.consistent = consistent;
        }

        public Map<String, Long> getLastCommittedTxs()
        {
            return Collections.unmodifiableMap( lastCommittedTxs );
        }

        public boolean isConsistent()
        {
            return consistent;
        }
    }

    static final String TOO_OLD_BACKUP = "It's been too long since this backup was last " + "updated, and it has " +
            "fallen too far behind the database transaction stream for incremental backup to be possible. You need to" +
            " perform a full backup at this point. " + "You can modify this time interval by setting the '" +
            GraphDatabaseSettings.keep_logical_logs.name() + "' configuration on the database to a higher value.";

    static final String DIFFERENT_STORE = "Target directory contains full backup of a logically different store.";

    private final FileSystemAbstraction fileSystem;
    private final StringLogger logger;

    BackupService() {
        this( new DefaultFileSystemAbstraction(), StringLogger.SYSTEM );
    }

    BackupService( FileSystemAbstraction fileSystem )
    {
        this( fileSystem, StringLogger.SYSTEM );
    }

    BackupService( FileSystemAbstraction fileSystem, StringLogger logger )
    {
        this.fileSystem = fileSystem;
        this.logger = logger;
    }

    BackupOutcome doFullBackup( final String sourceHostNameOrIp, final int sourcePort, String targetDirectory,
                                boolean checkConsistency, Config tuningConfiguration, final boolean forensics )

    {
        if ( directoryContainsDb( targetDirectory ) )
        {
            throw new RuntimeException( targetDirectory + " already contains a database" );
        }

        Map<String, String> params = tuningConfiguration.getParams();
        params.put( GraphDatabaseSettings.store_dir.name(), targetDirectory );
        tuningConfiguration.applyChanges( params );

        long timestamp = System.currentTimeMillis();
        Map<String, Long> lastCommittedTxs = new TreeMap<>();
        boolean consistent = !checkConsistency; // default to true if we're not checking consistency

        GraphDatabaseAPI targetDb = null;
        try
        {
            ConsoleLogger consoleLog = new ConsoleLogger( StringLogger.SYSTEM );
            RemoteStoreCopier storeCopier = new RemoteStoreCopier( tuningConfiguration, loadKernelExtensions(),
            consoleLog,new DevNullLoggingService(), new DefaultFileSystemAbstraction(), new Monitors());
            storeCopier.copyStore( new RemoteStoreCopier.StoreCopyRequester()
            {
                private BackupClient client;
                @Override
                public Response<?> copyStore( StoreWriter writer )
                {
                    client = new BackupClient( sourceHostNameOrIp, sourcePort, new DevNullLoggingService(),
                            new Monitors(), null );
                    client.start();
                    return client.fullBackup( writer, forensics );
                }

                @Override
                public void done()
                {
                    client.stop();
                }
            }, CancellationRequest.NONE );

            targetDb = startTemporaryDb( targetDirectory, VerificationLevel.NONE /* run full check instead */ );
            new LogicalLogSeeder(logger).ensureAtLeastOneLogicalLogPresent( sourceHostNameOrIp, sourcePort, targetDb );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            if ( targetDb != null )
            {
                targetDb.shutdown();
            }
        }
        bumpLogFile( targetDirectory, timestamp );
        if ( checkConsistency )
        {
            try
            {
                consistent = new ConsistencyCheckService().runFullConsistencyCheck(
                        targetDirectory,
                        tuningConfiguration,
                        ProgressMonitorFactory.textual( System.err ),
                        logger ).isSuccessful();
            }
            catch ( ConsistencyCheckIncompleteException e )
            {
                logger.error( "Consistency check incomplete", e );
            }
            finally
            {
                logger.flush();
            }
        }
        return new BackupOutcome( lastCommittedTxs, consistent );
    }

    BackupOutcome doIncrementalBackup( String sourceHostNameOrIp, int sourcePort, String targetDirectory,
                                       boolean verification ) throws IncrementalBackupNotPossibleException
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

    BackupOutcome doIncrementalBackupOrFallbackToFull( String sourceHostNameOrIp, int sourcePort, String targetDirectory,
                                                       boolean verification, Config config, boolean forensics )
    {
        if(!directoryContainsDb( targetDirectory ))
        {
            return doFullBackup( sourceHostNameOrIp, sourcePort, targetDirectory, verification, config, forensics );
        }

        try
        {
            return doIncrementalBackup( sourceHostNameOrIp, sourcePort, targetDirectory, verification );
        }
        catch(IncrementalBackupNotPossibleException e)
        {
            try
            {
                // Our existing backup is out of date.
                logger.info( "Existing backup is too far out of date, a new full backup will be performed." );

                File targetDirFile = new File( targetDirectory );
                FileUtils.deleteRecursively( targetDirFile );

                return doFullBackup( sourceHostNameOrIp, sourcePort, targetDirFile.getAbsolutePath(),
                        verification, config, forensics );
            }
            catch ( Exception fullBackupFailure )
            {
                throw new RuntimeException( "Failed to perform incremental backup, fell back to full backup, " +
                        "but that failed as well: '" + fullBackupFailure.getMessage() + "'.", fullBackupFailure );
            }
        }
    }

    BackupOutcome doIncrementalBackup( String sourceHostNameOrIp, int sourcePort, GraphDatabaseAPI targetDb )
            throws IncrementalBackupNotPossibleException
    {
        return incrementalWithContext( sourceHostNameOrIp, sourcePort, targetDb, slaveContextOf( targetDb ) );
    }

    private RequestContext slaveContextOf( GraphDatabaseAPI graphDb )
    {
        XaDataSourceManager dsManager = dsManager( graphDb );
        List<Tx> txs = new ArrayList<>();
        for ( XaDataSource ds : dsManager.getAllRegisteredDataSources() )
        {
            txs.add( RequestContext.lastAppliedTx( ds.getName(), ds.getLastCommittedTxId() ) );
        }
        return RequestContext.anonymous( txs.toArray( new Tx[txs.size()] ) );
    }

    boolean directoryContainsDb( String targetDirectory )
    {
        return fileSystem.fileExists( new File( targetDirectory, NeoStore.DEFAULT_NAME ) );
    }

    static GraphDatabaseAPI startTemporaryDb( String targetDirectory, ConfigParam... params )
    {
        Map<String, String> config = new HashMap<>();
        config.put( OnlineBackupSettings.online_backup_enabled.name(), Settings.FALSE );
        config.put( InternalAbstractGraphDatabase.Configuration.log_configuration_file.name(),
                "neo4j-backup-logback.xml" );
        for ( ConfigParam param : params )
        {
            if ( param != null )
            {
                param.configure( config );
            }
        }
        return (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( targetDirectory )
                .setConfig( config ).newGraphDatabase();
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
                                                  RequestContext context ) throws IncrementalBackupNotPossibleException
    {
        BackupClient client = new BackupClient( sourceHostNameOrIp, sourcePort,
                targetDb.getDependencyResolver().resolveDependency( Logging.class ),
                targetDb.getDependencyResolver().resolveDependency( Monitors.class ),
                targetDb.storeId() );
        client.start();

        Map<String, Long> lastCommittedTxs;
        boolean successfullyReadLastCommittedTxs = false;
        try
        {
            lastCommittedTxs = unpackResponse( client.incrementalBackup( context ),
                    targetDb.getDependencyResolver().resolveDependency( XaDataSourceManager.class ),
                    new ProgressTxHandler() );
            trimLogicalLogCount( targetDb );
            successfullyReadLastCommittedTxs = true;
        }
        catch ( MismatchingStoreIdException e )
        {
            throw new RuntimeException( DIFFERENT_STORE, e );
        }
        catch ( RuntimeException e )
        {
            if ( e.getCause() != null && e.getCause() instanceof NoSuchLogVersionException )
            {
                throw new IncrementalBackupNotPossibleException( TOO_OLD_BACKUP, e.getCause() );
            }
            else
            {
                throw new RuntimeException( "Failed to perform incremental backup.", e );
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
                if ( successfullyReadLastCommittedTxs )
                {
                    logger.warn( "Unable to stop backup client", throwable );
                }
                else
                {
                    throw new RuntimeException( "Unable to stop backup client", throwable );
                }
            }
        }
        return new BackupOutcome( lastCommittedTxs, true );
    }

    private void trimLogicalLogCount( GraphDatabaseAPI targetDb )
    {
        for ( XaDataSource ds : dsManager( targetDb ).getAllRegisteredDataSources() )
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

    private XaDataSourceManager dsManager( GraphDatabaseAPI targetDb )
    {
        return targetDb.getDependencyResolver().resolveDependency( XaDataSourceManager.class );
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
        SortedMap<String, Long> lastCommittedTxs = new TreeMap<>();
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

    private List<KernelExtensionFactory<?>> loadKernelExtensions()
    {
        List<KernelExtensionFactory<?>> kernelExtensions = new ArrayList<>();
        for ( KernelExtensionFactory factory : Service.load( KernelExtensionFactory.class ) )
        {
            kernelExtensions.add( factory );
        }
        return kernelExtensions;
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

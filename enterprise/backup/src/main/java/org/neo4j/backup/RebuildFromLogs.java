/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import java.io.IOException;
import java.util.List;

import org.neo4j.consistency.ConsistencyCheckSettings;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.FullCheck;
import org.neo4j.consistency.statistics.Statistics;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.direct.DirectStoreAccess;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.IndexUpdatesValidator;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.OnlineIndexUpdatesValidator;
import org.neo4j.kernel.impl.api.index.ValidatedIndexUpdates;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.IOCursor;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionCursor;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableVersionableLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReaderLogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.impl.transaction.state.PropertyLoader;
import org.neo4j.kernel.impl.util.IdOrderingQueue;
import org.neo4j.logging.FormattedLog;

import static java.lang.String.format;
import static org.neo4j.consistency.ConsistencyCheckService.defaultConsistencyCheckThreadsNumber;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.api.TransactionApplicationMode.EXTERNAL;
import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.log.PhysicalLogFile.openForVersion;
import static org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel.DEFAULT_READ_AHEAD_SIZE;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

class RebuildFromLogs
{
    private static final String FULL_CHECK = "full";
    private static final String UP_TO_TX_ID = "tx";

    private static final FileSystemAbstraction FS = new DefaultFileSystemAbstraction();

    private final StoreAccess stores;
    private final NeoStoreDataSource dataSource;
    private final TransactionRepresentationStoreApplier storeApplier;
    private final IndexUpdatesValidator indexUpdatesValidator;
    private final IndexingService indexingService;

    RebuildFromLogs( GraphDatabaseAPI graphdb )
    {
        DependencyResolver resolver = graphdb.getDependencyResolver();
        this.stores = new StoreAccess( graphdb );
        this.dataSource = resolver.resolveDependency( DataSourceManager.class ).getDataSource();
        this.storeApplier = resolver.resolveDependency( TransactionRepresentationStoreApplier.class )
                                    .withLegacyIndexTransactionOrdering( IdOrderingQueue.BYPASS );
        KernelHealth kernelHealth = resolver.resolveDependency( KernelHealth.class );
        PropertyLoader propertyLoader = new PropertyLoader( stores.getRawNeoStore() );
        this.indexingService = resolver.resolveDependency( IndexingService.class );
        this.indexUpdatesValidator = new OnlineIndexUpdatesValidator( stores.getRawNeoStore(), kernelHealth,
                propertyLoader, indexingService, IndexUpdateMode.BATCHED );
    }

    void applyTransactionsFrom( File sourceDir, long upToTxId ) throws IOException
    {
        PhysicalLogFiles logFiles = new PhysicalLogFiles( sourceDir, FS );
        int startVersion = 0;
        ReaderLogVersionBridge versionBridge = new ReaderLogVersionBridge( FS, logFiles );
        PhysicalLogVersionedStoreChannel startingChannel = openForVersion( logFiles, FS, startVersion );
        ReadableVersionableLogChannel channel =
                new ReadAheadLogChannel( startingChannel, versionBridge, DEFAULT_READ_AHEAD_SIZE );
        try ( IOCursor<CommittedTransactionRepresentation> cursor =
                      new PhysicalTransactionCursor<>( channel, new VersionAwareLogEntryReader<>() ) )
        {
            while ( cursor.next() )
            {
                long txId = cursor.get().getCommitEntry().getTxId();
                TransactionRepresentation transaction = cursor.get().getTransactionRepresentation();
                try ( LockGroup locks = new LockGroup();
                      ValidatedIndexUpdates indexUpdates = indexUpdatesValidator.validate( transaction ) )
                {
                    storeApplier.apply( transaction, indexUpdates, locks, txId, EXTERNAL );
                }
                if ( upToTxId != BASE_TX_ID && upToTxId == txId )
                {
                    return;
                }
            }
        }
        indexingService.flushAll();
    }

    public static void main( String[] args ) throws Exception
    {
        if ( args == null )
        {
            printUsage();
            return;
        }
        Args params = Args.withFlags( FULL_CHECK ).parse( args );
        @SuppressWarnings("boxing")
        boolean full = params.getBoolean( FULL_CHECK, false, true );
        long txId = params.getNumber( UP_TO_TX_ID, BASE_TX_ID ).longValue();
        List<String> orphans = params.orphans();
        args = orphans.toArray( new String[orphans.size()] );
        if ( args.length != 2 )
        {
            printUsage( "Exactly two positional arguments expected: "
                    + "<source dir with logs> <target dir for graphdb>, got " + args.length );
            System.exit( -1 );
            return;
        }
        File source = new File( args[0] ), target = new File( args[1] );
        if ( !source.isDirectory() )
        {
            printUsage( source + " is not a directory" );
            System.exit( -1 );
            return;
        }
        if ( target.exists() )
        {
            if ( target.isDirectory() )
            {
                if ( new BackupService().directoryContainsDb( target.getAbsoluteFile() ) )
                {
                    printUsage( "target graph database already exists" );
                    System.exit( -1 );
                    return;
                }
                System.err.println( "WARNING: the directory " + target + " already exists" );
            }
            else
            {
                printUsage( target + " is a file" );
                System.exit( -1 );
                return;
            }
        }

        try ( PageCache pageCache = StandalonePageCacheFactory.createPageCache( new DefaultFileSystemAbstraction() ) )
        {
            GraphDatabaseAPI graphdb =
                    BackupService.startTemporaryDb( target.getAbsoluteFile(), pageCache, stringMap() );
            try
            {
                PhysicalLogFiles logFiles = new PhysicalLogFiles( source, FS );
                long highestVersion = logFiles.getHighestLogVersion();
                if ( highestVersion < 0 )
                {
                    printUsage( "Inconsistent number of log files found in " + source );
                    return;
                }
                long txCount = findLastTransactionId( logFiles, highestVersion );
                ProgressMonitorFactory progress;
                if ( txCount < 0 )
                {
                    progress = ProgressMonitorFactory.NONE;
                    System.err.println(
                            "Unable to report progress, cannot find highest txId, attempting rebuild anyhow." );
                }
                else
                {
                    progress = ProgressMonitorFactory.textual( System.err );
                }
                progress.singlePart( format( "Rebuilding store from %s transactions ", txCount ), txCount );
                RebuildFromLogs rebuilder = new RebuildFromLogs( graphdb );
                rebuilder.applyTransactionsFrom( source, txId );

                // if we didn't run the full checker for each transaction, run it afterwards
                if ( !full )
                {
                    rebuilder.checkConsistency();
                }
            }
            finally
            {
                graphdb.shutdown();
            }
        }
    }

    private static long findLastTransactionId( PhysicalLogFiles logFiles, long highestVersion )
            throws IOException
    {
        ReadableVersionableLogChannel logChannel = new ReadAheadLogChannel(
                PhysicalLogFile.openForVersion( logFiles, FS, highestVersion ),
                NO_MORE_CHANNELS, DEFAULT_READ_AHEAD_SIZE );

        long lastTransactionId = -1;

        try ( IOCursor<CommittedTransactionRepresentation> cursor =
                      new PhysicalTransactionCursor<>( logChannel, new VersionAwareLogEntryReader<>() ) )
        {
            while (cursor.next())
            {
                lastTransactionId = cursor.get().getCommitEntry().getTxId();
            }
        }
        return lastTransactionId;
    }

    private void checkConsistency() throws ConsistencyCheckIncompleteException
    {
        Config tuningConfiguration = new Config( stringMap(),
                GraphDatabaseSettings.class, ConsistencyCheckSettings.class );
        new FullCheck( tuningConfiguration, ProgressMonitorFactory.textual( System.err ), Statistics.NONE, defaultConsistencyCheckThreadsNumber() )
                .execute( new DirectStoreAccess( stores, dataSource.getLabelScanStore(), dataSource.getDependencyResolver().resolveDependency( SchemaIndexProvider.class ) ),
                        FormattedLog.toOutputStream( System.out ) );
    }

    private static void printUsage( String... msgLines )
    {
        for ( String line : msgLines )
        {
            System.err.println( line );
        }
        System.err.println( Args.jarUsage( RebuildFromLogs.class, "[-full] <source dir with logs> <target dir for " +
                "graphdb>" ) );
        System.err.println( "WHERE:   <source dir>  is the path for where transactions to rebuild from are stored" );
        System.err.println( "         <target dir>  is the path for where to create the new graph database" );
        System.err.println( "         -full     --  to run a full check over the entire store for each transaction" );
        System.err.println( "         -tx       --  to rebuild the store up to a given transaction" );
    }
}

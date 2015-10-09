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
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.direct.DirectStoreAccess;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.BatchingTransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.api.LegacyIndexApplierLookup;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.OnlineIndexUpdatesValidator;
import org.neo4j.kernel.impl.api.index.ValidatedIndexUpdates;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory;
import org.neo4j.kernel.impl.store.NeoStore;
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
import org.neo4j.kernel.impl.util.StringLogger;

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.api.TransactionApplicationMode.EXTERNAL;
import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.log.PhysicalLogFile.openForVersion;
import static org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel.DEFAULT_READ_AHEAD_SIZE;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

class RebuildFromLogs
{
    public static final long ALL_TXS = BASE_TX_ID;
    public static final long ALL_LOGS = -1;
    private static final int BATCH_SIZE = 1000;

    private static final String UP_TO_TX_ID = "tx";
    private static final String UP_TO_LOG_VERSION = "ver";

    private final FileSystemAbstraction fs;

    public RebuildFromLogs( FileSystemAbstraction fs )
    {
        this.fs = fs;
    }

    public static void main( String[] args ) throws Exception
    {
        if ( args == null )
        {
            printUsage();
            return;
        }
        Args params = Args.parse( args );
        long txId = params.getNumber( UP_TO_TX_ID, ALL_TXS ).longValue();
        long logVersion = params.getNumber( UP_TO_LOG_VERSION, ALL_LOGS ).longValue();

        if ( logVersion != ALL_LOGS && txId != ALL_TXS )
        {
            System.err.printf( "We do not support both options '%s' and '%s' at the same time ",
                    UP_TO_TX_ID, UP_TO_LOG_VERSION );
            return;
        }

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
                if ( new BackupService().directoryContainsDb( target.getAbsolutePath() ) )
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

        new RebuildFromLogs( new DefaultFileSystemAbstraction() ).rebuild( source, target, txId, logVersion );
    }

    public void rebuild( File source, File target, long txId, long logVersion ) throws Exception
    {
        try ( PageCache pageCache = StandalonePageCacheFactory.createPageCache( fs ) )
        {
            PhysicalLogFiles logFiles = new PhysicalLogFiles( source, fs );
            long highestLogVersion = logFiles.getHighestLogVersion();
            if ( logVersion != ALL_LOGS )
            {
                highestLogVersion = Math.min( highestLogVersion, logVersion );
            }

            if ( highestLogVersion < 0 )
            {
                printUsage( "Inconsistent number of log files found in " + source );
                return;
            }

            long txCount = findLastTransactionId( logFiles, highestLogVersion );
            if ( txCount < 0 )
            {
                throw new IllegalStateException( "Cannot find txs in the transaction log files" );
            }

            try ( TransactionApplier applier = new TransactionApplier( fs, target, pageCache ) )
            {
                long lastTxId = applier.applyTransactionsFrom( source, txId, highestLogVersion );
                // set last tx id in neostore otherwise the db is not usable
                NeoStore.setRecord( fs, new File( target, NeoStore.DEFAULT_NAME ),
                        NeoStore.Position.LAST_TRANSACTION_ID, lastTxId );
            }

            try ( ConsistencyChecker checker = new ConsistencyChecker( target, pageCache ) )
            {
                checker.checkConsistency();
            }
        }
    }


    private long findLastTransactionId( PhysicalLogFiles logFiles, long highestVersion ) throws IOException
    {
        ReadableVersionableLogChannel logChannel = new ReadAheadLogChannel(
                PhysicalLogFile.openForVersion( logFiles, fs, highestVersion ),
                NO_MORE_CHANNELS, DEFAULT_READ_AHEAD_SIZE );

        long lastTransactionId = -1;

        try ( IOCursor<CommittedTransactionRepresentation> cursor =
                      new PhysicalTransactionCursor<>( logChannel, new VersionAwareLogEntryReader<>() ) )
        {
            while ( cursor.next() )
            {
                lastTransactionId = cursor.get().getCommitEntry().getTxId();
            }
        }
        return lastTransactionId;
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


    private static class TransactionApplier implements AutoCloseable
    {
        private final GraphDatabaseAPI graphdb;
        private final BatchingTransactionRepresentationStoreApplier storeApplier;
        private final OnlineIndexUpdatesValidator indexUpdatesValidator;
        private final NeoStore neoStore;
        private final IndexingService indexingService;
        private final FileSystemAbstraction fs;

        TransactionApplier( FileSystemAbstraction fs, File dbDirectory, PageCache pageCache )
        {
            this.fs = fs;
            this.graphdb = BackupService.startTemporaryDb( dbDirectory.getAbsolutePath(), pageCache, stringMap() );
            DependencyResolver resolver = graphdb.getDependencyResolver();
            this.neoStore = resolver.resolveDependency( NeoStore.class );

            this.indexingService = resolver.resolveDependency( IndexingService.class );
            // Instead of taking the transaction store applier from the db we construct a batching one to go faster!
            this.storeApplier = new BatchingTransactionRepresentationStoreApplier( indexingService,
                    resolver.resolveDependency( LabelScanStore.class ), neoStore,
                    resolver.resolveDependency( CacheAccessBackDoor.class ),
                    resolver.resolveDependency( LockService.class ),
                    resolver.resolveDependency( LegacyIndexApplierLookup.class ),
                    resolver.resolveDependency( IndexConfigStore.class ),
                    IdOrderingQueue.BYPASS
            );
            this.indexUpdatesValidator = new OnlineIndexUpdatesValidator(
                    neoStore, new PropertyLoader( neoStore ), indexingService, IndexUpdateMode.BATCHED );
        }

        long applyTransactionsFrom( File sourceDir, long upToTxId, long upToLogVersion ) throws IOException
        {
            PhysicalLogFiles logFiles = new PhysicalLogFiles( sourceDir, fs );
            int startVersion = 0;
            ReaderLogVersionBridge versionBridge = new ReaderLogVersionBridge( fs, logFiles );
            PhysicalLogVersionedStoreChannel startingChannel = openForVersion( logFiles, fs, startVersion );
            ReadableVersionableLogChannel channel =
                    new ReadAheadLogChannel( startingChannel, versionBridge, DEFAULT_READ_AHEAD_SIZE );
            long txId = BASE_TX_ID;
            long nextFlushOn = txId + BATCH_SIZE;
            try ( IOCursor<CommittedTransactionRepresentation> cursor =
                          new PhysicalTransactionCursor<>( channel, new VersionAwareLogEntryReader<>() ) )
            {
                while ( cursor.next() && channel.getVersion() <= upToLogVersion )
                {
                    txId = cursor.get().getCommitEntry().getTxId();
                    TransactionRepresentation transaction = cursor.get().getTransactionRepresentation();
                    try ( LockGroup locks = new LockGroup();
                          ValidatedIndexUpdates indexUpdates = indexUpdatesValidator.validate( transaction ) )
                    {
                        storeApplier.apply( transaction, indexUpdates, locks, txId, EXTERNAL );
                    }
                    if ( txId == nextFlushOn )
                    {
                        System.out.println( "Applied tx #" + txId );
                        storeApplier.closeBatch();
                        nextFlushOn += BATCH_SIZE;
                    }
                    if ( upToTxId != BASE_TX_ID && upToTxId == txId )
                    {
                        return txId;
                    }
                }
            }
            storeApplier.closeBatch();
            indexingService.flushAll();
            return txId;
        }

        @Override
        public void close()
        {
            graphdb.shutdown();
        }
    }

    private static class ConsistencyChecker implements AutoCloseable
    {
        private final GraphDatabaseAPI graphdb;
        private final NeoStoreDataSource dataSource;
        private final Config tuningConfiguration =
                new Config( stringMap(), GraphDatabaseSettings.class, ConsistencyCheckSettings.class );
        private SchemaIndexProvider indexes;

        ConsistencyChecker( File dbDirectory, PageCache pageCache )
        {
            this.graphdb = BackupService.startTemporaryDb( dbDirectory.getAbsolutePath(), pageCache, stringMap() );
            DependencyResolver resolver = graphdb.getDependencyResolver();
            this.dataSource = resolver.resolveDependency( DataSourceManager.class ).getDataSource();
            this.indexes = resolver.resolveDependency( SchemaIndexProvider.class );
        }

        private void checkConsistency() throws ConsistencyCheckIncompleteException
        {
            LabelScanStore labelScanStore = dataSource.getLabelScanStore();
            DirectStoreAccess stores = new DirectStoreAccess( new StoreAccess( graphdb ), labelScanStore, indexes );
            new FullCheck( tuningConfiguration, ProgressMonitorFactory.textual( System.err ) )
                    .execute( stores, StringLogger.SYSTEM );
        }

        @Override
        public void close() throws Exception
        {
            graphdb.shutdown();
        }
    }
}

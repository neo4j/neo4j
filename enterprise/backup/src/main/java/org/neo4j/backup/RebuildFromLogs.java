/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.direct.DirectStoreAccess;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.nioneo.store.StoreAccess;
import org.neo4j.kernel.impl.nioneo.xa.DataSourceManager;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.xaframework.IOCursor;
import org.neo4j.kernel.impl.transaction.xaframework.LogVersionBridge;
import org.neo4j.kernel.impl.transaction.xaframework.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.xaframework.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.xaframework.PhysicalTransactionCursor;
import org.neo4j.kernel.impl.transaction.xaframework.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.xaframework.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.xaframework.ReaderLogVersionBridge;
import org.neo4j.kernel.impl.transaction.xaframework.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.util.StringLogger;

import static java.lang.String.format;

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.transaction.xaframework.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.xaframework.ReadAheadLogChannel.DEFAULT_READ_AHEAD_SIZE;

class RebuildFromLogs
{
    private static final FileSystemAbstraction FS = new DefaultFileSystemAbstraction();

    private final StoreAccess stores;
    private final NeoStoreXaDataSource dataSource;
    private final TransactionRepresentationStoreApplier storeApplier;

    RebuildFromLogs( GraphDatabaseAPI graphdb )
    {
        this.stores = new StoreAccess( graphdb );
        this.dataSource = graphdb.getDependencyResolver().resolveDependency( DataSourceManager.class ).getDataSource();
        this.storeApplier = graphdb.getDependencyResolver().resolveDependency(
                TransactionRepresentationStoreApplier.class );
    }

    RebuildFromLogs applyTransactionsFrom( ProgressListener progress, File sourceDir ) throws IOException
    {
        PhysicalLogFiles logFiles = new PhysicalLogFiles( sourceDir, FS );
        int startVersion = 0;
        File logFile = logFiles.getLogFileForVersion( startVersion ); // assume we always start from version 0?
        LogVersionBridge versionBridge = new ReaderLogVersionBridge( FS, logFiles );
        ReadableLogChannel logChannel = new ReadAheadLogChannel(
                new PhysicalLogVersionedStoreChannel( FS.open( logFile, "R" ), startVersion ),
                versionBridge, DEFAULT_READ_AHEAD_SIZE );

        try (IOCursor<CommittedTransactionRepresentation> cursor = new PhysicalTransactionCursor( logChannel,
                new VersionAwareLogEntryReader() ) )
        {
            while (cursor.next())
            {
                storeApplier.apply( cursor.get().getTransactionRepresentation(), cursor.get().getCommitEntry().getTxId(), true );
            }
        }

        return this;
    }

    public static void main( String[] args ) throws Exception
    {
        if ( args == null )
        {
            printUsage();
            return;
        }
        Args params = new Args( args );
        @SuppressWarnings("boxing")
        boolean full = params.getBoolean( "full", false, true );
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

        GraphDatabaseAPI graphdb = BackupService.startTemporaryDb( target.getAbsolutePath() );
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
                System.err.println( "Unable to report progress, cannot find highest txId, attempting rebuild anyhow." );
            }
            else
            {
                progress = ProgressMonitorFactory.textual( System.err );
            }
            ProgressListener listener = progress.singlePart(
                    format( "Rebuilding store from %s transactions ", txCount ), txCount );
            RebuildFromLogs rebuilder = new RebuildFromLogs( graphdb ).applyTransactionsFrom( listener, source );
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

    private static long findLastTransactionId( PhysicalLogFiles logFiles, long highestVersion )
            throws IOException
    {
        File logFile = logFiles.getLogFileForVersion( highestVersion );
        ReadableLogChannel logChannel = new ReadAheadLogChannel(
                new PhysicalLogVersionedStoreChannel( FS.open( logFile, "R" ), highestVersion ),
                NO_MORE_CHANNELS, DEFAULT_READ_AHEAD_SIZE );

        long lastTransactionId = -1;

        try (IOCursor<CommittedTransactionRepresentation> cursor = new PhysicalTransactionCursor( logChannel,
                new VersionAwareLogEntryReader() ))
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
        new FullCheck( tuningConfiguration, ProgressMonitorFactory.textual( System.err ) )
                .execute( new DirectStoreAccess( stores, dataSource.getLabelScanStore(), dataSource.getIndexProvider() ),
                        StringLogger.SYSTEM );
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
    }
}

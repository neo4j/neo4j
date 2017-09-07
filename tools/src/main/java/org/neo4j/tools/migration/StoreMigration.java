/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.tools.migration;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.extension.KernelExtensions;
import org.neo4j.kernel.extension.dependency.AllByPrioritySelectionStrategy;
import org.neo4j.kernel.impl.api.index.SchemaIndexProviderMap;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.logging.StoreLogService;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.spi.SimpleKernelContext;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.storemigration.DatabaseMigrator;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.storemigration.monitoring.VisibleMigrationProgressMonitor;
import org.neo4j.kernel.impl.storemigration.participant.StoreMigrator;
import org.neo4j.kernel.impl.transaction.log.FlushablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.LogHeaderCache;
import org.neo4j.kernel.impl.transaction.log.LogTailScanner;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyLogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.state.DefaultSchemaIndexProviderMap;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.spi.explicitindex.IndexImplementation;
import org.neo4j.kernel.spi.explicitindex.IndexProviders;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_internal_log_path;
import static org.neo4j.kernel.extension.UnsatisfiedDependencyStrategies.ignore;
import static org.neo4j.kernel.impl.pagecache.ConfigurableStandalonePageCacheFactory.createPageCache;

/**
 * Stand alone tool for migrating/upgrading a neo4j database from one version to the next.
 *
 * @see StoreMigrator
 */
//: TODO introduce abstract tool class as soon as we will have several tools in tools module
public class StoreMigration
{
    private static final String HELP_FLAG = "help";

    public static void main( String[] args ) throws IOException
    {
        Args arguments = Args.withFlags( HELP_FLAG ).parse( args );
        if ( arguments.getBoolean( HELP_FLAG, false ) || args.length == 0 )
        {
            printUsageAndExit();
        }
        File storeDir = parseDir( arguments );

        FormattedLogProvider userLogProvider = FormattedLogProvider.toOutputStream( System.out );
        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {
            new StoreMigration().run( fileSystem, storeDir, getMigrationConfig(), userLogProvider );
        }
    }

    private static Config getMigrationConfig()
    {
        return Config.defaults( GraphDatabaseSettings.allow_upgrade, Settings.TRUE);
    }

    public void run( final FileSystemAbstraction fs, final File storeDirectory, Config config,
            LogProvider userLogProvider ) throws IOException
    {
        StoreLogService logService = StoreLogService.withUserLogProvider( userLogProvider )
                .withInternalLog( config.get( store_internal_log_path ) ).build( fs );

        VisibleMigrationProgressMonitor progressMonitor =
                new VisibleMigrationProgressMonitor( logService.getUserLog( StoreMigration.class ) );

        LifeSupport life = new LifeSupport();

        life.add( logService );

        // Add participants from kernel extensions...
        ExplicitIndexProvider explicitIndexProvider = new ExplicitIndexProvider();

        Log log = userLogProvider.getLog( StoreMigration.class );
        try ( PageCache pageCache = createPageCache( fs, config ) )
        {
            Dependencies deps = new Dependencies();
            deps.satisfyDependencies( fs, config, explicitIndexProvider, pageCache, logService, new Monitors(),
                    RecoveryCleanupWorkCollector.IMMEDIATE );

            KernelContext kernelContext = new SimpleKernelContext( storeDirectory, DatabaseInfo.UNKNOWN, deps );
            KernelExtensions kernelExtensions = life.add( new KernelExtensions(
                    kernelContext, GraphDatabaseDependencies.newDependencies().kernelExtensions(),
                    deps, ignore() ) );

            final PhysicalLogFiles logFiles = new PhysicalLogFiles( storeDirectory, PhysicalLogFile.DEFAULT_NAME, fs );
            LogTailScanner tailScanner = new LogTailScanner( logFiles, fs, new VersionAwareLogEntryReader<>() );

            // Add the kernel store migrator
            life.start();

            AllByPrioritySelectionStrategy<SchemaIndexProvider> indexProviderSelection = new AllByPrioritySelectionStrategy<>();
            SchemaIndexProvider defaultIndexProvider = kernelExtensions.resolveDependency( SchemaIndexProvider.class,
                    indexProviderSelection );
            SchemaIndexProviderMap schemaIndexProviderMap = new DefaultSchemaIndexProviderMap( defaultIndexProvider,
                    indexProviderSelection.lowerPrioritizedCandidates() );

            long startTime = System.currentTimeMillis();
            DatabaseMigrator migrator = new DatabaseMigrator( progressMonitor, fs, config, logService,
                    schemaIndexProviderMap, explicitIndexProvider.getIndexProviders(),
                    pageCache, RecordFormatSelector.selectForConfig( config, userLogProvider ), tailScanner );
            migrator.migrate( storeDirectory );

            // Append checkpoint so the last log entry will have the latest version
            appendCheckpoint( fs, storeDirectory, pageCache, logFiles, tailScanner );

            long duration = System.currentTimeMillis() - startTime;
            log.info( format( "Migration completed in %d s%n", duration / 1000 ) );
        }
        catch ( Exception e )
        {
            throw new StoreUpgrader.UnableToUpgradeException( "Failure during upgrade", e );
        }
        finally
        {
            life.shutdown();
        }
    }

    private void appendCheckpoint( FileSystemAbstraction fs, File storeDirectory, PageCache pageCache,
            PhysicalLogFiles logFiles, LogTailScanner tailScanner ) throws IOException
    {
        ReadOnlyLogVersionRepository logVersionRepository = new ReadOnlyLogVersionRepository( pageCache, storeDirectory );
        ReadOnlyTransactionIdStore
                readOnlyTransactionIdStore = new ReadOnlyTransactionIdStore( pageCache, storeDirectory );
        PhysicalLogFile logFile = new PhysicalLogFile( fs, logFiles, Long.MAX_VALUE /*don't rotate*/,
                () -> readOnlyTransactionIdStore.getLastClosedTransactionId() - 1, logVersionRepository,
                PhysicalLogFile.NO_MONITOR,
                new LogHeaderCache( 10 ) );

        try ( Lifespan lifespan = new Lifespan( logFile ) )
        {
            FlushablePositionAwareChannel writer = logFile.getWriter();
            TransactionLogWriter transactionLogWriter = new TransactionLogWriter( new LogEntryWriter( writer ) );
            transactionLogWriter.checkPoint( tailScanner.getTailInformation().lastCheckPoint.getLogPosition() );
            writer.prepareForFlush().flush();
        }
    }

    private class ExplicitIndexProvider implements IndexProviders
    {
        private final Map<String,IndexImplementation> indexProviders = new HashMap<>();

        public Map<String,IndexImplementation> getIndexProviders()
        {
            return indexProviders;
        }

        @Override
        public void registerIndexProvider( String name, IndexImplementation index )
        {
            indexProviders.put( name, index );
        }

        @Override
        public boolean unregisterIndexProvider( String name )
        {
            IndexImplementation removed = indexProviders.remove( name );
            return removed != null;
        }
    }

    private static File parseDir( Args args )
    {
        if ( args.orphans().size() != 1 )
        {
            System.out.println( "Error: too much arguments provided." );
            printUsageAndExit();
        }
        File dir = new File( args.orphans().get( 0 ) );
        if ( !dir.isDirectory() )
        {
            System.out.println( "Invalid directory: '" + dir + "'" );
            printUsageAndExit();
        }
        return dir;
    }

    private static void printUsageAndExit()
    {
        System.out.println( "Store migration tool performs migration of a store in specified location to latest " +
                            "supported store version." );
        System.out.println();
        System.out.println( "Options:" );
        System.out.println( "-help    print this help message" );
        System.out.println();
        System.out.println( "Usage:" );
        System.out.println( "./storeMigration [option] <store directory>" );
        System.exit( 1 );
    }
}

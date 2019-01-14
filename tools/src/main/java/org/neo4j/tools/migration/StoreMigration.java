/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.extension.KernelExtensions;
import org.neo4j.kernel.extension.dependency.AllByPrioritySelectionStrategy;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
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
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.state.DefaultIndexProviderMap;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.recovery.LogTailScanner;
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
            new StoreMigration().run( fileSystem, storeDir, getMigrationConfig( storeDir ), userLogProvider );
        }
    }

    private static Config getMigrationConfig( File storeDir )
    {
        Config config = Config.defaults( GraphDatabaseSettings.allow_upgrade, Settings.TRUE );
        config.augment( GraphDatabaseSettings.neo4j_home, storeDir.getAbsolutePath() );
        return config;
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
            Monitors monitors = new Monitors();
            deps.satisfyDependencies( fs, config, explicitIndexProvider, pageCache, logService, monitors,
                    RecoveryCleanupWorkCollector.immediate() );

            KernelContext kernelContext = new SimpleKernelContext( storeDirectory, DatabaseInfo.UNKNOWN, deps );
            KernelExtensions kernelExtensions = life.add( new KernelExtensions(
                    kernelContext, GraphDatabaseDependencies.newDependencies().kernelExtensions(),
                    deps, ignore() ) );

            final LogFiles logFiles = LogFilesBuilder.activeFilesBuilder( storeDirectory, fs, pageCache )
                    .withConfig( config ).build();
            LogTailScanner tailScanner = new LogTailScanner( logFiles, new VersionAwareLogEntryReader<>(), monitors );

            // Add the kernel store migrator
            life.start();

            AllByPrioritySelectionStrategy<IndexProvider> indexProviderSelection = new AllByPrioritySelectionStrategy<>();
            IndexProvider defaultIndexProvider = kernelExtensions.resolveDependency( IndexProvider.class,
                    indexProviderSelection );
            IndexProviderMap indexProviderMap = new DefaultIndexProviderMap( defaultIndexProvider,
                    indexProviderSelection.lowerPrioritizedCandidates() );

            long startTime = System.currentTimeMillis();
            DatabaseMigrator migrator = new DatabaseMigrator( progressMonitor, fs, config, logService,
                    indexProviderMap, explicitIndexProvider.getIndexProviders(),
                    pageCache, RecordFormatSelector.selectForConfig( config, userLogProvider ), tailScanner );
            migrator.migrate( storeDirectory );

            // Append checkpoint so the last log entry will have the latest version
            appendCheckpoint( logFiles, tailScanner );

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

    private void appendCheckpoint( LogFiles logFiles, LogTailScanner tailScanner ) throws IOException
    {
        try ( Lifespan lifespan = new Lifespan( logFiles ) )
        {
            FlushablePositionAwareChannel writer = logFiles.getLogFile().getWriter();
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

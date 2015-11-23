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
package org.neo4j.tools.migration;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.IndexImplementation;
import org.neo4j.graphdb.index.IndexProviders;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensions;
import org.neo4j.kernel.extension.dependency.HighestSelectionStrategy;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.StoreLogService;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.storemigration.ConfigMapUpgradeConfiguration;
import org.neo4j.kernel.impl.storemigration.LegacyIndexMigrator;
import org.neo4j.kernel.impl.storemigration.SchemaIndexMigrator;
import org.neo4j.kernel.impl.storemigration.StoreMigrator;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.UpgradableDatabase;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.monitoring.VisibleMigrationProgressMonitor;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;
import static org.neo4j.kernel.extension.UnsatisfiedDependencyStrategies.ignore;
import static org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory.createPageCache;
import static org.neo4j.kernel.impl.storemigration.StoreUpgrader.NO_MONITOR;

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
        StoreMigration migration = new StoreMigration();
        migration.run( new DefaultFileSystemAbstraction(), storeDir, getMigrationConfig(),
                userLogProvider, NO_MONITOR );
    }

    private static Config getMigrationConfig()
    {
        return new Config( MapUtil.stringMap( GraphDatabaseSettings.allow_store_upgrade.name(), "true" ) );
    }

    public void run( final FileSystemAbstraction fs, final File storeDirectory, Config config,
            LogProvider userLogProvider, StoreUpgrader.Monitor monitor ) throws IOException
    {
        ConfigMapUpgradeConfiguration upgradeConfiguration = new ConfigMapUpgradeConfiguration( config );
        StoreUpgrader migrationProcess = new StoreUpgrader( upgradeConfiguration, fs, monitor, userLogProvider );

        LifeSupport life = new LifeSupport();

        // Add participants from kernel extensions...
        LegacyIndexProvider indexProvider = new LegacyIndexProvider();

        Dependencies deps = new Dependencies();
        deps.satisfyDependencies( fs, config );
        deps.satisfyDependencies( indexProvider );

        KernelContext kernelContext = new MigrationKernelContext( fs, storeDirectory );
        KernelExtensions kernelExtensions = life.add( new KernelExtensions(
                kernelContext, GraphDatabaseDependencies.newDependencies().kernelExtensions(),
                deps, ignore() ) );

        LogService logService =
                StoreLogService.withUserLogProvider( userLogProvider ).inStoreDirectory( fs, storeDirectory );

        // Add the kernel store migrator
        life.start();
        SchemaIndexProvider schemaIndexProvider = kernelExtensions.resolveDependency( SchemaIndexProvider.class,
                HighestSelectionStrategy.getInstance() );

        LabelScanStoreProvider labelScanStoreProvider = kernelExtensions
                .resolveDependency( LabelScanStoreProvider.class, HighestSelectionStrategy.getInstance() );

        Log log = userLogProvider.getLog( StoreMigration.class );
        try ( PageCache pageCache = createPageCache( fs, config ) )
        {
            UpgradableDatabase upgradableDatabase =
                    new UpgradableDatabase( fs, new StoreVersionCheck( pageCache ), new LegacyStoreVersionCheck( fs ) );
            migrationProcess.addParticipant( new SchemaIndexMigrator( fs ) );
            migrationProcess.addParticipant( new LegacyIndexMigrator( fs, indexProvider.getIndexProviders(),
                    userLogProvider ) );
            migrationProcess.addParticipant( new StoreMigrator(
                    new VisibleMigrationProgressMonitor( logService.getInternalLog( StoreMigration.class ) ),
                    fs, pageCache, config, logService ) );
            // Perform the migration
            long startTime = System.currentTimeMillis();
            migrationProcess
                    .migrateIfNeeded( storeDirectory, upgradableDatabase, schemaIndexProvider, labelScanStoreProvider );
            long duration = System.currentTimeMillis() - startTime;
            log.info( format( "Migration completed in %d s%n", duration / 1000 ) );
        }
        catch ( IOException e )
        {
            throw new StoreUpgrader.UnableToUpgradeException( "Failure during upgrade", e );
        }
        finally
        {
            life.shutdown();
        }
    }

    private class MigrationKernelContext implements KernelContext
    {
        private final FileSystemAbstraction fileSystem;
        private final File storeDirectory;

        public MigrationKernelContext( FileSystemAbstraction fileSystem, File storeDirectory )
        {
            this.fileSystem = fileSystem;
            this.storeDirectory = storeDirectory;
        }

        @Override
        public FileSystemAbstraction fileSystem()
        {
            return fileSystem;
        }

        @Override
        public File storeDir()
        {
            return storeDirectory;
        }
    }

    private class LegacyIndexProvider implements IndexProviders
    {

        private Map<String,IndexImplementation> indexProviders = new HashMap<>();

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
            System.out.println("Error: too much arguments provided.");
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
        System.out.println("Options:");
        System.out.println("-help    print this help message");
        System.out.println();
        System.out.println("Usage:");
        System.out.println( "./storeMigration [option] <store directory>" );
        System.exit( 1 );
    }
}

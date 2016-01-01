/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration;

import java.io.File;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultGraphDatabaseDependencies;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensions;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.storemigration.monitoring.VisibleMigrationProgressMonitor;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.logging.SystemOutLogging;

import static java.lang.String.format;
import static org.neo4j.kernel.extension.UnsatisfiedDependencyStrategies.ignore;
import static org.neo4j.kernel.impl.storemigration.StoreUpgrader.NO_MONITOR;

/**
 * Stand alone tool for migrating/upgrading a neo4j database from one version to the next.
 *
 * @see StoreMigrator
 */
public class StoreMigrationTool
{
    public static void main( String[] args )
    {
        String legacyStoreDirectory = args[0];
        new StoreMigrationTool().run( legacyStoreDirectory, new Config(), new SystemOutLogging(), NO_MONITOR );
    }

    public void run( String legacyStoreDirectory, Config config, Logging logging, StoreUpgrader.Monitor monitor )
    {
        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        ConfigMapUpgradeConfiguration upgradeConfiguration = new ConfigMapUpgradeConfiguration( config );
        StoreUpgrader migrationProcess = new StoreUpgrader( upgradeConfiguration, fs, monitor, logging );

        // Add the kernel store migrator
        config = StoreFactory.configForStoreDir( config, new File( legacyStoreDirectory ) );
        migrationProcess.addParticipant( new StoreMigrator(
                new VisibleMigrationProgressMonitor( logging.getMessagesLog( StoreMigrationTool.class ), System.out ),
                new UpgradableDatabase( new StoreVersionCheck( fs ) ),
                config, logging ) );

        // Add participants from kernel extensions...
        LifeSupport life = new LifeSupport();
        KernelExtensions kernelExtensions = life.add( new KernelExtensions(
                new DefaultGraphDatabaseDependencies().kernelExtensions(), config,
                kernelExtensionDependencyResolver( fs, config ), ignore() ) );
        life.start();
        // ... TODO although hard coded to SchemaIndexProvider a.t.m.
        try
        {
            SchemaIndexProvider schemaIndexProvider = kernelExtensions.resolveDependency( SchemaIndexProvider.class,
                    SchemaIndexProvider.HIGHEST_PRIORITIZED_OR_NONE );
            migrationProcess.addParticipant( schemaIndexProvider.storeMigrationParticipant() );
        }
        catch ( IllegalArgumentException e )
        {   // That's fine actually, no schema index provider on the classpath or something
        }

        // Perform the migration
        try
        {
            long startTime = System.currentTimeMillis();
            migrationProcess.migrateIfNeeded( new File( legacyStoreDirectory ) );
            long duration = System.currentTimeMillis() - startTime;
            logging.getMessagesLog( StoreMigrationTool.class )
                    .info( format( "Migration completed in %d s%n", duration / 1000 ) );
        }
        finally
        {
            life.shutdown();
        }
    }

    private DependencyResolver kernelExtensionDependencyResolver(
            final FileSystemAbstraction fileSystem, final Config config )
    {
        return new DependencyResolver.Adapter()
        {
            @Override
            public <T> T resolveDependency( Class<T> type, SelectionStrategy selector ) throws IllegalArgumentException
            {
                if ( type.isInstance( fileSystem ) )
                {
                    return type.cast( fileSystem );
                }
                if ( type.isInstance( config ) )
                {
                    return type.cast( config );
                }
                throw new IllegalArgumentException( type.toString() );
            }
        };
    }
}

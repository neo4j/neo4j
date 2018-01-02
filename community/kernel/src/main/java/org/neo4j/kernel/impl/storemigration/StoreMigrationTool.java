/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensions;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.StoreLogService;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.spi.SimpleKernelContext;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.monitoring.VisibleMigrationProgressMonitor;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.udc.UsageDataKeys;

import static java.lang.String.format;
import static org.neo4j.kernel.extension.UnsatisfiedDependencyStrategies.ignore;
import static org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory.createPageCache;
import static org.neo4j.kernel.impl.storemigration.StoreUpgrader.NO_MONITOR;

/**
 * Stand alone tool for migrating/upgrading a neo4j database from one version to the next.
 *
 * @see StoreMigrator
 */
public class StoreMigrationTool
{
    public static void main( String[] args ) throws IOException
    {
        String legacyStoreDirectory = args[0];
        FormattedLogProvider userLogProvider = FormattedLogProvider.toOutputStream( System.out );
        new StoreMigrationTool().run( new DefaultFileSystemAbstraction(), new File( legacyStoreDirectory ),
                new Config(), userLogProvider, NO_MONITOR );
    }

    public void run( final FileSystemAbstraction fs, final File legacyStoreDirectory, Config config,
            LogProvider userLogProvider, StoreUpgrader.Monitor monitor ) throws IOException
    {
        ConfigMapUpgradeConfiguration upgradeConfiguration = new ConfigMapUpgradeConfiguration( config );
        StoreUpgrader migrationProcess = new StoreUpgrader( upgradeConfiguration, fs, monitor, userLogProvider );

        LifeSupport life = new LifeSupport();

        // Add participants from kernel extensions...
        Dependencies deps = new Dependencies();
        deps.satisfyDependencies( fs, config );


        KernelContext kernelContext = new SimpleKernelContext(  fs, legacyStoreDirectory,
                UsageDataKeys.OperationalMode.single );
        KernelExtensions kernelExtensions = life.add( new KernelExtensions(
                kernelContext, GraphDatabaseDependencies.newDependencies().kernelExtensions(),
                deps, ignore() ) );

        LogService logService =
                StoreLogService.withUserLogProvider( userLogProvider ).inStoreDirectory( fs, legacyStoreDirectory );

        // Add the kernel store migrator
        life.start();
        SchemaIndexProvider schemaIndexProvider = kernelExtensions.resolveDependency( SchemaIndexProvider.class,
                SchemaIndexProvider.HIGHEST_PRIORITIZED_OR_NONE );

        Log log = userLogProvider.getLog( StoreMigrationTool.class );
        try ( PageCache pageCache = createPageCache( fs, config ) )
        {
            UpgradableDatabase upgradableDatabase =
                    new UpgradableDatabase( new StoreVersionCheck( pageCache ), new LegacyStoreVersionCheck( fs ) );
            migrationProcess.addParticipant( new StoreMigrator(
                    new VisibleMigrationProgressMonitor( logService.getInternalLog( StoreMigrationTool.class ) ),
                    fs, pageCache, config, logService ) );
            migrationProcess.addParticipant(
                    schemaIndexProvider.storeMigrationParticipant( fs, pageCache ) );
            // Perform the migration
            long startTime = System.currentTimeMillis();
            migrationProcess.migrateIfNeeded( legacyStoreDirectory, upgradableDatabase, schemaIndexProvider );
            long duration = System.currentTimeMillis() - startTime;
            log.info( format( "Migration completed in %d s%n", duration / 1000 ) );
        }
        catch ( IOException e )
        {
            throw new StoreUpgrader.UnableToUpgradeException( "Failure during upgrade", e );
        }
        catch ( IllegalArgumentException e )
        {   // That's fine actually, no schema index provider on the classpath or something
        }
        finally
        {
            life.shutdown();
        }
    }
}

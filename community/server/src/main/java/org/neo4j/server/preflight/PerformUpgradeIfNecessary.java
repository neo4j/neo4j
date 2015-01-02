/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.server.preflight;

import java.io.File;
import java.io.PrintStream;
import java.util.Map;

import org.apache.commons.configuration.Configuration;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.storemigration.ConfigMapUpgradeConfiguration;
import org.neo4j.kernel.impl.storemigration.CurrentDatabase;
import org.neo4j.kernel.impl.storemigration.DatabaseFiles;
import org.neo4j.kernel.impl.storemigration.StoreMigrator;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.UpgradableDatabase;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByConfigurationException;
import org.neo4j.kernel.impl.storemigration.monitoring.VisibleMigrationProgressMonitor;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.server.configuration.Configurator;

public class PerformUpgradeIfNecessary implements PreflightTask
{
    private String failureMessage = "Unable to upgrade database";
    private final Configuration config;
    private final PrintStream out;
    private final Map<String, String> dbConfig;
    private final ConsoleLogger log;

    public PerformUpgradeIfNecessary( Configuration serverConfig, Map<String, String> dbConfig, PrintStream out,
            Logging logging )
    {
        this.config = serverConfig;
        this.dbConfig = dbConfig;
        this.out = out;
        this.log = logging.getConsoleLog( getClass() );
    }

    @Override
    public boolean run()
    {
        try
        {
            String dbLocation = new File( config.getString( Configurator.DATABASE_LOCATION_PROPERTY_KEY ) )
                    .getAbsolutePath();

            if ( new CurrentDatabase(new StoreVersionCheck( new DefaultFileSystemAbstraction() ) ).storeFilesAtCurrentVersion( new File( dbLocation ) ) )
            {
                return true;
            }

            File store = new File( dbLocation, NeoStore.DEFAULT_NAME);
            dbConfig.put( "store_dir", dbLocation );
            dbConfig.put( "neo_store", store.getPath() );

            FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
            UpgradableDatabase upgradableDatabase = new UpgradableDatabase( new StoreVersionCheck( fileSystem ) );
            if ( !upgradableDatabase.storeFilesUpgradeable( store ) )
            {
                return true;
            }

            Config conf = new Config( dbConfig, GraphDatabaseSettings.class );
            StoreUpgrader storeUpgrader = new StoreUpgrader( conf,
                    new ConfigMapUpgradeConfiguration( conf ),
                    upgradableDatabase, new StoreMigrator( new VisibleMigrationProgressMonitor( StringLogger.DEV_NULL, out ) ),
                    new DatabaseFiles( fileSystem ), new DefaultIdGeneratorFactory(), fileSystem );

            try
            {
                storeUpgrader.attemptUpgrade( store );
            }
            catch ( UpgradeNotAllowedByConfigurationException e )
            {
                log.log( e.getMessage() );
                out.println( e.getMessage() );
                failureMessage = e.getMessage();
                return false;
            }
            catch ( StoreUpgrader.UnableToUpgradeException e )
            {
                log.error( "Unable to upgrade store", e );
                return false;
            }
            return true;
        }
        catch ( Exception e )
        {
            log.error( "Unknown error", e );
            return false;
        }
    }

    @Override
    public String getFailureMessage()
    {
        return failureMessage;
    }
}

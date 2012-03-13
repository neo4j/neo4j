/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.server.storemigration;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Properties;
import org.neo4j.kernel.CommonFactories;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.storemigration.ConfigMapUpgradeConfiguration;
import org.neo4j.kernel.impl.storemigration.CurrentDatabase;
import org.neo4j.kernel.impl.storemigration.DatabaseFiles;
import org.neo4j.kernel.impl.storemigration.StoreMigrator;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.storemigration.UpgradableDatabase;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByConfigurationException;
import org.neo4j.kernel.impl.storemigration.monitoring.VisibleMigrationProgressMonitor;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.PropertyFileConfigurator;
import org.neo4j.server.configuration.validation.DatabaseLocationMustBeSpecifiedRule;
import org.neo4j.server.configuration.validation.Validator;
import org.neo4j.server.logging.Logger;

public class PreStartupStoreUpgrader
{
    public static void main( String[] args ) throws IOException
    {
        PreStartupStoreUpgrader preStartupStoreUpgrader =
                new PreStartupStoreUpgrader( System.getProperties(), System.out );
        int exit = preStartupStoreUpgrader.run();
        if ( exit != 0 )
        {
            System.exit( exit );
        }
    }

    private final Logger logger = Logger.getLogger( PreStartupStoreUpgrader.class );
    private Properties systemProperties;
    private PrintStream out;

    public PreStartupStoreUpgrader( Properties systemProperties, PrintStream out )
    {
        this.systemProperties = systemProperties;
        this.out = out;
    }

    public int run()
    {
        try
        {
            Configurator configurator = getConfigurator();
            HashMap<String, String> config = new HashMap<String, String>( configurator.getDatabaseTuningProperties() );

            String dbLocation = new File( configurator.configuration()
                    .getString( Configurator.DATABASE_LOCATION_PROPERTY_KEY ) ).getAbsolutePath();

            if ( new CurrentDatabase().storeFilesAtCurrentVersion( new File( dbLocation ) ) )
            {
                return 0;
            }

            String separator = System.getProperty( "file.separator" );
            String store = dbLocation + separator + NeoStore.DEFAULT_NAME;
            config.put( "store_dir", dbLocation );
            config.put( "neo_store", store );

            if ( !new UpgradableDatabase().storeFilesUpgradeable( new File( store ) ) )
            {
                logger.info( "Store files missing, or not in suitable state for upgrade. " +
                        "Leaving this problem for main server process to resolve." );
                return 0;
            }
            
            Config conf = new Config( StringLogger.SYSTEM, config );
            StoreUpgrader storeUpgrader = new StoreUpgrader( conf, StringLogger.SYSTEM,new ConfigMapUpgradeConfiguration( conf ),
                    new UpgradableDatabase(), new StoreMigrator( new VisibleMigrationProgressMonitor( out ) ),
                    new DatabaseFiles(), CommonFactories.defaultIdGeneratorFactory(), CommonFactories.defaultFileSystemAbstraction() );

            try
            {
                storeUpgrader.attemptUpgrade( store );
            }
            catch ( UpgradeNotAllowedByConfigurationException e )
            {
                logger.info( e.getMessage() );
                out.println( e.getMessage() );
                return 1;
            }
            catch ( StoreUpgrader.UnableToUpgradeException e )
            {
                logger.error( e );
                return 1;
            }
            return 0;
        }
        catch ( Exception e )
        {
            logger.error( e );
            return 1;
        }
    }

    protected Configurator getConfigurator()
    {
        File configFile = new File( systemProperties.getProperty( Configurator.NEO_SERVER_CONFIG_FILE_KEY,
                Configurator.DEFAULT_CONFIG_DIR ) );
        return new PropertyFileConfigurator( new Validator( new DatabaseLocationMustBeSpecifiedRule() ), configFile );
    }

}

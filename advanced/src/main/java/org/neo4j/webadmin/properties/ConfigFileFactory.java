/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.webadmin.properties;

import java.io.File;
import java.io.IOException;

import org.neo4j.rest.domain.DatabaseLocator;

public class ConfigFileFactory
{

    public static final String APP_ARGS_DEV_PATH = "target/appargs";
    public static final String JVM_ARGS_DEV_PATH = "target/jvmargs";

    public static final String DB_CONFIG_PATH = "neo4j.properties";

    public static final String BACKUP_CONFIG_PATH = "backup.json";
    public static final String BACKUP_LOG_PATH = "backup.log.json";

    public static final String CONFIG_FOLDER = "./conf/";

    public static final String GENERAL_CONFIG_PATH = CONFIG_FOLDER
                                                     + "server.properties";
    public static final String SERVICE_CONFIG_PATH = CONFIG_FOLDER
                                                     + "wrapper.conf";

    /**
     * Get database config file, creating one if it does not exist.
     * 
     * @return
     * @throws IOException
     */
    public static File getDbConfigFile() throws IOException
    {
        File configFile = new File( new File(
                DatabaseLocator.getDatabaseLocation() ), DB_CONFIG_PATH );

        return ensureFileExists( configFile );
    }

    /**
     * Get startup config file, creating one if it does not exist.
     * 
     * @return
     * @throws IOException
     */
    public static File getGeneralConfigFile() throws IOException
    {
        File configFile = new File( GENERAL_CONFIG_PATH );

        return ensureFileExists( configFile );
    }

    /**
     * Get file that stores JVM startup arguments during development.
     * 
     * @return
     * @throws IOException
     */
    public static File getDevelopmentJvmArgsFile() throws IOException
    {
        return ensureFileExists( new File( JVM_ARGS_DEV_PATH ) );
    }

    /**
     * Get file that stores app startup arguments during development.
     * 
     * @return
     * @throws IOException
     */
    public static File getDevelopmentAppArgsFile() throws IOException
    {
        return ensureFileExists( new File( APP_ARGS_DEV_PATH ) );
    }

    /**
     * Get the service configuration file, this is where JVM args are changed
     * when running in production. This method is slightly different from the
     * above, it does not try to create a file if one does not exist. It assumes
     * that if there is no service config file, the environment we're running in
     * is not the production service env.
     * 
     * @return the service file or null if no file exists.
     * @throws IOException
     */
    public static File getServiceConfigFile() throws IOException
    {
        File configFile = new File( SERVICE_CONFIG_PATH );

        if ( !configFile.exists() )
        {
            return null;
        }

        return configFile;
    }

    public static File getBackupConfigFile() throws IOException
    {
        File file;
        if ( DatabaseLocator.isLocalDatabase() )
        {

            file = new File( new File( DatabaseLocator.getDatabaseLocation() ),
                    BACKUP_CONFIG_PATH );
        }
        else
        {
            file = new File( new File( CONFIG_FOLDER ), BACKUP_CONFIG_PATH );
        }
        return ensureFileExists( file );
    }

    public static File getBackupLogFile() throws IOException
    {
        File file;
        if ( DatabaseLocator.isLocalDatabase() )
        {

            file = new File( new File( DatabaseLocator.getDatabaseLocation() ),
                    BACKUP_LOG_PATH );
        }
        else
        {
            file = new File( new File( CONFIG_FOLDER ), BACKUP_LOG_PATH );
        }
        return ensureFileExists( file );
    }

    //
    // INTERNALS
    //

    private static synchronized File ensureFileExists( File file )
            throws IOException
    {
        if ( !file.exists() )
        {
            if ( file.getParent() != null )
            {
                new File( file.getParent() ).mkdirs();
            }

            if ( !file.createNewFile() )
            {
                throw new IllegalStateException( file.getAbsolutePath() );
            }
        }

        return file;
    }

}

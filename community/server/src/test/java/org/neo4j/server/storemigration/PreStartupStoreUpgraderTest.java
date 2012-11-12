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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.util.FileUtils.copyRecursively;
import static org.neo4j.kernel.impl.util.FileUtils.deleteRecursively;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.util.Properties;

import org.junit.Test;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.server.configuration.Configurator;

public class PreStartupStoreUpgraderTest
{
    public static final String HOME_DIRECTORY = "target/" + PreStartupStoreUpgraderTest.class.getSimpleName();
    public static final String STORE_DIRECTORY = HOME_DIRECTORY + "/data/graph.db";

    @Test
    public void shouldExitImmediatelyIfStoreIsAlreadyAtLatestVersion() throws IOException
    {
        Properties systemProperties = buildProperties( false );
        new EmbeddedGraphDatabase( STORE_DIRECTORY ).shutdown();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        PreStartupStoreUpgrader upgrader = new PreStartupStoreUpgrader( systemProperties,
                new PrintStream( outputStream ) );

        int exit = upgrader.run();

        assertEquals( 0, exit );

        assertEquals( "", new String( outputStream.toByteArray() ) );
    }

    @Test
    public void shouldGiveHelpfulMessageIfAutoUpgradeParameterNotSet() throws IOException
    {
        Properties systemProperties = buildProperties( false );
        prepareSampleLegacyDatabase( new File( STORE_DIRECTORY ) );
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        PreStartupStoreUpgrader upgrader = new PreStartupStoreUpgrader( systemProperties,
                new PrintStream( outputStream ) );

        int exit = upgrader.run();

        assertEquals( 1, exit );

        String[] lines = new String( outputStream.toByteArray() ).split( "\\r?\\n" );
        assertTrue( lines[0].contains( "To enable automatic upgrade, please set configuration parameter " +
                "\"allow_store_upgrade=true\"" ) );
    }

    @Test
    public void shouldExitCleanlyIfDatabaseMissingSoThatDatabaseCreationIsLeftToMainProcess() throws IOException
    {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        PreStartupStoreUpgrader upgrader = new PreStartupStoreUpgrader( buildProperties( true ),
                new PrintStream( outputStream ) );

        int exit = upgrader.run();

        assertEquals( 0, exit );

        assertEquals( "", new String( outputStream.toByteArray() ) );
    }

    @Test
    public void shouldUpgradeDatabase() throws IOException
    {
        Properties systemProperties = buildProperties( true );
        prepareSampleLegacyDatabase( new File( STORE_DIRECTORY ) );
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        PreStartupStoreUpgrader upgrader = new PreStartupStoreUpgrader( systemProperties,
                new PrintStream( outputStream ) );

        int exit = upgrader.run();

        assertEquals( 0, exit );

        String[] lines = new String( outputStream.toByteArray() ).split( "\\r?\\n" );
        assertEquals( "Starting upgrade of database store files", lines[0] );
        assertEquals( dots(100), lines[1] );
        assertEquals( "Finished upgrade of database store files", lines[2] );
    }

    private Properties buildProperties(boolean allowStoreUpgrade) throws IOException
    {
        FileUtils.deleteRecursively( new File( HOME_DIRECTORY ) );
        new File( HOME_DIRECTORY + "/conf" ).mkdirs();

        Properties databaseProperties = new Properties();
        if (allowStoreUpgrade)
        {
            databaseProperties.setProperty( Config.ALLOW_STORE_UPGRADE, "true" );
        }
        String databasePropertiesFileName = HOME_DIRECTORY + "/conf/neo4j.properties";
        databaseProperties.store( new FileWriter( databasePropertiesFileName ), null );

        Properties serverProperties = new Properties();
        serverProperties.setProperty( Configurator.DATABASE_LOCATION_PROPERTY_KEY, STORE_DIRECTORY );
        serverProperties.setProperty( Configurator.DB_TUNING_PROPERTY_FILE_KEY, databasePropertiesFileName );
        String serverPropertiesFileName = HOME_DIRECTORY + "/conf/neo4j-server.properties";
        serverProperties.store( new FileWriter( serverPropertiesFileName ), null );

        Properties systemProperties = new Properties();
        systemProperties.put( Configurator.NEO_SERVER_CONFIG_FILE_KEY, serverPropertiesFileName );
        return systemProperties;
    }

    public static void prepareSampleLegacyDatabase( File workingDirectory ) throws IOException
    {
        File resourceDirectory = findOldFormatStoreDirectory();

        deleteRecursively( workingDirectory );
        assertTrue( workingDirectory.mkdirs() );

        copyRecursively( resourceDirectory, workingDirectory );
    }

    public static File findOldFormatStoreDirectory()
    {
        URL legacyStoreResource = PreStartupStoreUpgraderTest.class.getResource( "legacystore/exampledb/neostore" );
        return new File( legacyStoreResource.getFile() ).getParentFile();
    }

    private String dots( int count )
    {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < count; i++) {
            builder.append( "." );
        }
        return builder.toString();
    }
}

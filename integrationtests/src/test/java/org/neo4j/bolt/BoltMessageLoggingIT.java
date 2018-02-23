/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.bolt;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.auth_enabled;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.bolt_log_filename;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.bolt_logging_enabled;
import static org.neo4j.kernel.configuration.BoltConnector.EncryptionLevel.DISABLED;
import static org.neo4j.kernel.configuration.Connector.ConnectorType.BOLT;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.kernel.configuration.Settings.TRUE;

public class BoltMessageLoggingIT
{
    private static final String CONNECTOR_KEY = "bolt";

    @Rule
    public final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();

    @Rule
    public final DatabaseRule db = new ImpermanentDatabaseRule()
    {
        @Override
        protected void configure( GraphDatabaseFactory databaseFactory )
        {
            super.configure( databaseFactory );
            ((TestGraphDatabaseFactory) databaseFactory).setFileSystem( fs );
        }

        @Override
        protected void configure( GraphDatabaseBuilder builder )
        {
            super.configure( builder );
            builder.setConfig( auth_enabled, "false" );
            builder.setConfig( new BoltConnector( CONNECTOR_KEY ).type, BOLT.toString() );
            builder.setConfig( new BoltConnector( CONNECTOR_KEY ).enabled, TRUE );
            builder.setConfig( new BoltConnector( CONNECTOR_KEY ).listen_address, "localhost:0" );
            builder.setConfig( new BoltConnector( CONNECTOR_KEY ).encryption_level, DISABLED.toString() );
        }
    }.withSetting( OnlineBackupSettings.online_backup_enabled, Settings.FALSE ).startLazily();

    private Driver driver;

    @After
    public void closeDriver()
    {
        if ( driver != null )
        {
            driver.close();
        }
    }

    @Test
    public void shouldWriteToDefaultFileWhenEnabled() throws IOException
    {
        db.setConfig( bolt_logging_enabled, TRUE );
        db.ensureStarted();
        driver = newDriver();

        File boltLogFile = config().get( bolt_log_filename );
        assertBoltLogIsWritten( boltLogFile );
    }

    @Test
    public void shouldWriteNothingWhenDisabled()
    {
        db.setConfig( bolt_logging_enabled, FALSE );
        db.ensureStarted();
        driver = newDriver();

        File boltLogFile = config().get( bolt_log_filename );
        assertFalse( fs.fileExists( boltLogFile ) );

        try ( Session session = driver.session() )
        {
            session.run( "CREATE ()" ).consume();
        }

        assertFalse( fs.fileExists( boltLogFile ) );
    }

    @Test
    public void shouldWriteToCustomFileWhenConfigured() throws IOException
    {
        File customBoltLogFile = customBoltLogFile();

        db.setConfig( bolt_logging_enabled, TRUE );
        db.setConfig( bolt_log_filename, customBoltLogFile.toString() );
        db.ensureStarted();
        driver = newDriver();

        assertBoltLogIsWritten( customBoltLogFile );
    }

    @Test
    public void shouldWriteErrorsToCustomFileWhenConfigured() throws IOException
    {
        File customBoltLogFile = customBoltLogFile();

        db.setConfig( bolt_logging_enabled, TRUE );
        db.setConfig( bolt_log_filename, customBoltLogFile.toString() );
        db.ensureStarted();
        driver = newDriver();

        assertTrue( fs.fileExists( customBoltLogFile ) );

        String query = "RETURN 1 as 2";
        try ( Session session = driver.session() )
        {
            session.run( query ).consume();
            fail( "Should have failed" );
        }
        catch ( Exception e )
        {
            String contents = readFile( customBoltLogFile );
            assertThat( contents, containsString( "S FAILURE" ) );
        }
    }

    private void assertBoltLogIsWritten( File boltLogFile ) throws IOException
    {
        assertTrue( fs.fileExists( boltLogFile ) );

        String query = "CREATE (n:Person {name: 'Beta Ray Bill'}) \n" +
                "RETURN 42";
        try ( Session session = driver.session() )
        {
            session.run( query ).consume();
        }

        String contents = readFile( boltLogFile );
        assertThat( contents, containsString( "C RUN -" ) );
    }

    private String readFile( File file ) throws IOException
    {
        return IOUtils.toString( fs.openAsInputStream( file ) );
    }

    private Config config()
    {
        return resolveDependency( Config.class );
    }

    private Driver newDriver()
    {
        org.neo4j.driver.v1.Config driverConfig = org.neo4j.driver.v1.Config.build()
                .withoutEncryption().toConfig();

        return GraphDatabase.driver( boltUri(), driverConfig );
    }

    private URI boltUri()
    {
        HostnamePort localAddress = resolveDependency( ConnectorPortRegister.class ).getLocalAddress( CONNECTOR_KEY );
        return URI.create( "bolt://" + localAddress );
    }

    private <T> T resolveDependency( Class<T> type )
    {
        return graphDbApi().getDependencyResolver().resolveDependency( type );
    }

    private GraphDatabaseAPI graphDbApi()
    {
        return db.getGraphDatabaseAPI();
    }

    private static File customBoltLogFile()
    {
        return Paths.get( "tmp", "my_bolt.log" ).toAbsolutePath().toFile();
    }
}

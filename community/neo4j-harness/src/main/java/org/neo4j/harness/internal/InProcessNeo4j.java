/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.harness.internal;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.harness.Neo4j;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

@PublicApi
public class InProcessNeo4j implements Neo4j
{
    private final Path serverFolder;
    private final Path userLogFile;
    private final Path internalLogFile;
    private final DatabaseManagementService managementService;
    private final Config config;
    private final Closeable additionalClosable;
    private ConnectorPortRegister connectorPortRegister;

    /**
     * @deprecated Use {@link #InProcessNeo4j(Path, Path, Path, DatabaseManagementService, Config, Closeable)}.
     */
    @Deprecated( forRemoval = true )
    public InProcessNeo4j( File serverFolder, File userLogFile, File internalLogFile, DatabaseManagementService managementService, Config config,
            Closeable additionalClosable )
    {
        this( serverFolder.toPath(), userLogFile.toPath(), internalLogFile.toPath(), managementService, config, additionalClosable );
    }

    public InProcessNeo4j( Path serverFolder, Path userLogFile, Path internalLogFile, DatabaseManagementService managementService, Config config,
            Closeable additionalClosable )
    {
        this.serverFolder = serverFolder;
        this.userLogFile = userLogFile;
        this.internalLogFile = internalLogFile;
        this.managementService = managementService;
        this.config = config;
        this.additionalClosable = additionalClosable;
    }

    @Override
    public URI boltURI()
    {
        if ( config.get( BoltConnector.enabled ) )
        {
            return connectorUri( "bolt", BoltConnector.NAME );
        }
        throw new IllegalStateException( "Bolt connector is not configured" );
    }

    @Override
    public URI httpURI()
    {
        if ( config.get( HttpConnector.enabled ) )
        {
            return connectorUri( "http", HttpConnector.NAME );
        }
        throw new IllegalStateException( "HTTP connector is not configured" );
    }

    @Override
    public URI httpsURI()
    {
        if ( config.get( HttpsConnector.enabled ) )
        {
            return connectorUri( "https", HttpsConnector.NAME );
        }
        throw new IllegalStateException( "HTTPS connector is not configured" );
    }

    public void start()
    {
        this.connectorPortRegister = connectorPortRegister();
    }

    @Override
    public void close()
    {
        managementService.shutdown();
        this.connectorPortRegister = null;
        try
        {
            additionalClosable.close();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        try
        {
            if ( looksLikeMd5Hash( serverFolder.getFileName().toString() ) )
            {
                FileUtils.deleteDirectory( serverFolder );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Failed to clean up test server directory.", e );
        }
    }

    @Override
    public void printLogs( PrintStream out )
    {
        printLog( "User Log File", userLogFile, out );
        printLog( "Internal Log File", internalLogFile, out );
    }

    private static void printLog( String description, Path file, PrintStream out )
    {
        if ( file != null && Files.exists( file ) )
        {
            out.println( String.format( "---------- BEGIN %s ----------", description ) );

            try
            {
                Files.readAllLines( file ).forEach( out::println );
            }
            catch ( IOException ex )
            {
                out.println( "Unable to collect log files: " + ex.getMessage() );
                ex.printStackTrace( out );
            }
            finally
            {
                out.println( String.format( "---------- END %s ----------", description ) );
            }
        }
    }

    private boolean looksLikeMd5Hash( String name )
    {
        // Pure paranoia, and a silly check - but this decreases the likelihood that we delete something that isn't
        // our randomly generated folder significantly.
        return name.length() == 32;
    }

    @Override
    public DatabaseManagementService databaseManagementService()
    {
        return managementService;
    }

    @Override
    public GraphDatabaseService defaultDatabaseService()
    {
        return managementService.database( config.get( GraphDatabaseSettings.default_database ) );
    }

    @Override
    public Configuration config()
    {
        return config;
    }

    private URI connectorUri( String scheme, String connectorName )
    {
        HostnamePort hostPort = connectorPortRegister.getLocalAddress( connectorName );
        return URI.create( scheme + "://" + hostPort + "/" );
    }

    private ConnectorPortRegister connectorPortRegister()
    {
        return ((GraphDatabaseAPI) defaultDatabaseService() ).getDependencyResolver().resolveDependency( ConnectorPortRegister.class );
    }
}

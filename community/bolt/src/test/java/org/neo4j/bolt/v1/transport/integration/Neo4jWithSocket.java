/*
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
package org.neo4j.bolt.v1.transport.integration;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.bolt.BoltKernelExtension;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.BoltConnector.EncryptionLevel.OPTIONAL;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.boltConnector;

public class Neo4jWithSocket implements TestRule
{
    private final Consumer<Map<Setting<?>,String>> configure;
    TestGraphDatabaseFactory graphDatabaseFactory;
    private GraphDatabaseService gdb;

    public Neo4jWithSocket()
    {
        this( new TestGraphDatabaseFactory(), settings -> {} );
    }

    public Neo4jWithSocket( Consumer<Map<Setting<?>, String>> configure )
    {
        this( new TestGraphDatabaseFactory(), configure );
    }

    public Neo4jWithSocket( TestGraphDatabaseFactory graphDatabaseFactory, Consumer<Map<Setting<?>, String>> configure )
    {
        this.graphDatabaseFactory = graphDatabaseFactory;
        this.configure = configure;
    }

    @Override
    public Statement apply( final Statement statement, Description description )
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                restartDatabase( settings -> {} );
                try
                {
                    statement.evaluate();
                }
                finally
                {
                    shutdownDatabase();
                }
            }
        };
    }

    private void shutdownDatabase()
    {
        gdb.shutdown();
        gdb = null;
    }

    public void restartDatabase( Consumer<Map<Setting<?>, String>> overrideSettingsFunction ) throws IOException
    {
        if ( gdb != null )
        {
            gdb.shutdown();
        }
        Neo4jWithSocket.cleanupTemporaryTestFiles();
        Map<Setting<?>,String> settings = configure( overrideSettingsFunction );
        gdb = graphDatabaseFactory.newImpermanentDatabase( settings );
    }

    public Map<Setting<?>,String> configure(  Consumer<Map<Setting<?>, String>> overrideSettingsFunction ) throws IOException
    {
        Map<Setting<?>, String> settings = new HashMap<>();
        settings.put( boltConnector( "0" ).enabled, "true" );
        settings.put( boltConnector( "0" ).encryption_level, OPTIONAL.name() );
        settings.put( BoltKernelExtension.Settings.tls_key_file, tempPath( "key", ".key" ) );
        settings.put( BoltKernelExtension.Settings.tls_certificate_file, tempPath( "cert", ".cert" ) );
        configure.andThen( overrideSettingsFunction ).accept( settings );
        return settings;
    }

    private String tempPath(String prefix, String suffix ) throws IOException
    {
        Path path = Files.createTempFile( prefix, suffix );
        // We don't want an existing file just the path to a temporary file
        // a little silly to do it this way
        Files.delete( path );
        return path.toString();
    }

    public GraphDatabaseService graphDatabaseService()
    {
        return gdb;
    }

    public static void cleanupTemporaryTestFiles() throws IOException
    {
        for ( String name : new String[]{"roles", "auth"} )
        {
            Path file = Paths.get( "target/test-data/impermanent-db/data/dbms/" + name );
            if ( Files.exists( file ) )
            {
                Files.delete( file );
            }
        }
    }
}

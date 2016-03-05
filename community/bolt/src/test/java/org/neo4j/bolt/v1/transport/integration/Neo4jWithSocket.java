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
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.bolt.BoltKernelExtension;
import org.neo4j.bolt.security.auth.AuthUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.BoltConnector.EncryptionLevel.OPTIONAL;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.boltConnector;

public class Neo4jWithSocket implements TestRule
{
    private final Consumer<Map<Setting<?>,String>> configure;
    private StoreId storeId;

    public Neo4jWithSocket()
    {
        this( settings -> {} );
    }

    public Neo4jWithSocket( Consumer<Map<Setting<?>, String>> configure )
    {
        this.configure = configure;
    }

    public String uniqueIdentier()
    {
        return AuthUtils.uniqueIdentifier( storeId );
    }


    @Override
    public Statement apply( final Statement statement, Description description )
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                Map<Setting<?>, String> settings = new HashMap<>();
                settings.put( boltConnector( "0" ).enabled, "true" );
                settings.put( boltConnector( "0" ).encryption_level, OPTIONAL.name() );
                settings.put( BoltKernelExtension.Settings.tls_key_file, tempPath( "key", ".key" ) );
                settings.put( BoltKernelExtension.Settings.tls_certificate_file, tempPath( "cert", ".cert" ) );
                configure.accept( settings );
                final GraphDatabaseService gdb = new TestGraphDatabaseFactory().newImpermanentDatabase( settings );
                storeId = ((GraphDatabaseFacade) gdb).storeId();
                try
                {
                    statement.evaluate();
                }
                finally
                {
                    gdb.shutdown();
                }
            }
        };
    }

    private String tempPath(String prefix, String suffix ) throws IOException
    {
        Path path = Files.createTempFile( prefix, suffix );
        // We don't want an existing file just the path to a temporary file
        // a little silly to do it this way
        Files.delete( path );
        return path.toString();
    }
}

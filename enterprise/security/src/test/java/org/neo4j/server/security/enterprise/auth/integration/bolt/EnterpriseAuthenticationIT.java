/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.server.security.enterprise.auth.integration.bolt;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.bolt.v1.transport.integration.AuthenticationIT;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;
import org.neo4j.test.TestGraphDatabaseFactory;

public class EnterpriseAuthenticationIT extends AuthenticationIT
{
    @Override
    protected TestGraphDatabaseFactory getTestGraphDatabaseFactory()
    {
        return new TestEnterpriseGraphDatabaseFactory( logProvider );
    }

    @Override
    protected Consumer<Map<String, String>> getSettingsFunction()
    {
        final Path homeDir;
        try
        {
            homeDir = Files.createTempDirectory( "logs" );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Test setup failed to create temporary directory", e );
        }

        return settings ->
        {
            settings.put( GraphDatabaseSettings.auth_enabled.name(), "true" );
            settings.put( GraphDatabaseSettings.logs_directory.name(), homeDir.toAbsolutePath().toString() );
        };
    }

    @Override
    public void shouldFailIfMalformedAuthTokenUnknownScheme()
    {
        // Ignore this test in enterprise since custom schemes may be allowed
    }
}

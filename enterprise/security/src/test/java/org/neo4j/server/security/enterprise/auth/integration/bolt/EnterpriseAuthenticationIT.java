/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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

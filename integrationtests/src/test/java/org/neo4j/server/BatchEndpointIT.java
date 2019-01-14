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
package org.neo4j.server;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.configuration.ssl.LegacySslPolicyConfig;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.server.configuration.ServerSettings;

import static org.junit.Assert.assertEquals;
import static org.neo4j.server.ServerTestUtils.getRelativePath;
import static org.neo4j.server.ServerTestUtils.getSharedTestTemporaryFolder;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;
import static org.neo4j.test.server.HTTP.Response;
import static org.neo4j.test.server.HTTP.withBaseUri;

public class BatchEndpointIT
{
    @Rule
    public final Neo4jRule neo4j = new Neo4jRule()
            .withConfig( LegacySslPolicyConfig.certificates_directory,
                    getRelativePath( getSharedTestTemporaryFolder(), LegacySslPolicyConfig.certificates_directory ) )
            .withConfig( GraphDatabaseSettings.logs_directory,
                    getRelativePath( getSharedTestTemporaryFolder(), GraphDatabaseSettings.logs_directory ) )
            .withConfig( ServerSettings.http_logging_enabled, "true" )
            .withConfig( GraphDatabaseSettings.auth_enabled, "false" )
            .withConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
            .withConfig( ServerSettings.script_enabled, Settings.TRUE );

    @Test
    public void requestsShouldNotFailWhenHttpLoggingIsOn()
    {
        // Given
        String body = "[" +
                "{'method': 'POST', 'to': '/node', 'body': {'age': 1}, 'id': 1} ]";

        // When
        Response response = withBaseUri( neo4j.httpURI() )
                .withHeaders( "Content-Type", "application/json" )
                .POST( "db/data/batch", quotedJson( body ) );

        // Then
        assertEquals( 200, response.status() );
    }
}

/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.server.enterprise.jmx;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.util.Optional;

import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.CommunityBootstrapper;
import org.neo4j.server.NeoServer;
import org.neo4j.server.configuration.ConfigLoader;
import org.neo4j.server.enterprise.EnterpriseNeoServer;
import org.neo4j.server.enterprise.helpers.EnterpriseServerBuilder;
import org.neo4j.test.rule.CleanupRule;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.helpers.collection.Pair.pair;

public class ServerManagementTest
{

    private final CleanupRule cleanup = new CleanupRule();
    private final TestDirectory baseDir = TestDirectory.testDirectory();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( baseDir )
            .around( cleanup );

    @Test
    public void shouldBeAbleToRestartServer() throws Exception
    {
        // Given
        String dataDirectory1 = baseDir.directory( "data1" ).getAbsolutePath();
        String dataDirectory2 = baseDir.directory( "data2" ).getAbsolutePath();

        Config config = ConfigLoader.loadConfig(
                Optional.of( baseDir.directory() ),
                EnterpriseServerBuilder
                        .server()
                        .withDefaultDatabaseTuning()
                        .usingDataDir( dataDirectory1 )
                        .createConfigFiles(),
                pair( GraphDatabaseSettings.logs_directory.name(), baseDir.directory( "logs" ).getPath() ) );

        // When
        NeoServer server = cleanup.add( new EnterpriseNeoServer( config, graphDbDependencies(), NullLogProvider
                .getInstance() ) );
        server.start();

        assertNotNull( server.getDatabase().getGraph() );
        assertEquals( config.get( DatabaseManagementSystemSettings.database_path ).getAbsolutePath(),
                server.getDatabase().getLocation() );

        // Change the database location
        config.augment( stringMap( DatabaseManagementSystemSettings.data_directory.name(), dataDirectory2 ) );
        ServerManagement bean = new ServerManagement( server );
        bean.restartServer();

        // Then
        assertNotNull( server.getDatabase().getGraph() );
        assertEquals( config.get( DatabaseManagementSystemSettings.database_path ).getAbsolutePath(),
                server.getDatabase().getLocation() );
    }

    private static GraphDatabaseDependencies graphDbDependencies()
    {
        return GraphDatabaseDependencies.newDependencies().userLogProvider( NullLogProvider.getInstance() );
    }
}

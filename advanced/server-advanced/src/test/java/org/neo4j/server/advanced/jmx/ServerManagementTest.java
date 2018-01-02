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
package org.neo4j.server.advanced.jmx;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.NeoServer;
import org.neo4j.server.advanced.AdvancedNeoServer;
import org.neo4j.server.advanced.helpers.AdvancedServerBuilder;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.BaseServerConfigLoader;
import org.neo4j.test.CleanupRule;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class ServerManagementTest
{
    @Rule
    public final CleanupRule cleanup = new CleanupRule();
    @Rule
    public final TargetDirectory.TestDirectory baseDir = TargetDirectory.testDirForTest( getClass() );

    @Test
    public void shouldBeAbleToRestartServer() throws Exception
    {
        // Given
        String dbDirectory1 = baseDir.directory( "db1" ).getAbsolutePath();
        String dbDirectory2 = baseDir.directory( "db2" ).getAbsolutePath();

        Config config = new BaseServerConfigLoader().loadConfig( null,
                AdvancedServerBuilder
                        .server()
                        .withDefaultDatabaseTuning()
                        .usingDatabaseDir( dbDirectory1 )
                        .createPropertiesFiles(), NullLog.getInstance() );

        // When
        NeoServer server = cleanup.add( new AdvancedNeoServer( config, graphDbDependencies(), NullLogProvider.getInstance() ) );
        server.start();

        assertNotNull( server.getDatabase().getGraph() );
        assertEquals( dbDirectory1, server.getDatabase().getLocation() );

        // Change the database location
        config.augment( stringMap( Configurator.DATABASE_LOCATION_PROPERTY_KEY, dbDirectory2 ) );
        ServerManagement bean = new ServerManagement( server );
        bean.restartServer();

        // Then
        assertNotNull( server.getDatabase().getGraph() );
        assertEquals( dbDirectory2, server.getDatabase().getLocation() );
    }

    private static GraphDatabaseDependencies graphDbDependencies()
    {
        return GraphDatabaseDependencies.newDependencies().userLogProvider( NullLogProvider.getInstance() );
    }
}

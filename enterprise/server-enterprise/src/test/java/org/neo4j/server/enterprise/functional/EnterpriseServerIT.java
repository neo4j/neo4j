/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.server.enterprise.functional;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.server.NeoServer;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.enterprise.EnterpriseDatabase;
import org.neo4j.server.enterprise.helpers.EnterpriseServerBuilder;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class EnterpriseServerIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    @Rule
    public DumpPortListenerOnNettyBindFailure dumpPorts = new DumpPortListenerOnNettyBindFailure();

    @Test
    public void shouldBeAbleToStartInHAMode() throws Throwable
    {
        // Given
        File tuningFile = createNeo4jProperties();

        NeoServer server = EnterpriseServerBuilder.server()
                .usingDatabaseDir( folder.getRoot().getAbsolutePath() )
                .withProperty( Configurator.DB_MODE_KEY, "HA" )
                .withProperty( Configurator.DB_TUNING_PROPERTY_FILE_KEY, tuningFile.getAbsolutePath() )
                .persistent()
                .build();

        try
        {
            server.start();
            server.getDatabase();

            assertThat( server.getDatabase(), is( EnterpriseDatabase.class ) );
            assertThat( server.getDatabase().getGraph(), is( HighlyAvailableGraphDatabase.class ) );
        }
        finally
        {
            server.stop();
        }
    }

    private File createNeo4jProperties() throws IOException
    {
        File tuningFile = folder.newFile( "neo4j-test.properties" );
        FileOutputStream fos = new FileOutputStream( tuningFile );
        try
        {
            Properties neo4jProps = new Properties();

            neo4jProps.put( ClusterSettings.server_id.name(), "1" );
            neo4jProps.put( ClusterSettings.initial_hosts.name(), ":5001" );

            neo4jProps.store( fos, "" );
            return tuningFile;
        }
        finally
        {
            fos.close();
        }
    }

    @After
    public void whoListensOn5001()
    {
        dumpPorts.dumpListenersOn( 5001 );
    }
}

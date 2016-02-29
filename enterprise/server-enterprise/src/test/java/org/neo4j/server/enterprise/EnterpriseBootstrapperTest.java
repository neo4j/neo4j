/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.server.enterprise;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.server.BaseBootstrapperTest;
import org.neo4j.server.Bootstrapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.auth_store;
import static org.neo4j.server.CommunityBootstrapper.start;
import static org.neo4j.server.ServerTestUtils.getRelativePath;
import static org.neo4j.server.configuration.ServerSettings.data_directory;
import static org.neo4j.server.configuration.ServerSettings.tls_certificate_file;
import static org.neo4j.server.configuration.ServerSettings.tls_key_file;

public class EnterpriseBootstrapperTest extends BaseBootstrapperTest
{
    @Override
    protected Bootstrapper newBootstrapper()
    {
        return new EnterpriseBootstrapper();
    }

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void shouldBeAbleToStartInSingleMode() throws Exception
    {
        // When
        int resultCode = start( bootstrapper, commandLineConfig(
                "-c", configOption( EnterpriseServerSettings.mode.name(), "SINGLE" ),
                "-c", configOption( data_directory.name(), getRelativePath( folder.getRoot(), data_directory ) ),
                "-c", configOption( auth_store.name(), getRelativePath( folder.getRoot(), auth_store ) ),
                "-c", configOption( tls_key_file.name(), getRelativePath( folder.getRoot(), tls_key_file ) ),
                "-c", configOption( tls_certificate_file.name(), getRelativePath( folder.getRoot(), tls_certificate_file ) )
        ));

        // Then
        assertEquals( Bootstrapper.OK, resultCode );
        assertNotNull( bootstrapper.getServer() );
    }

    @Test
    public void shouldBeAbleToStartInHAMode() throws Exception
    {
        // When
        int resultCode = start( bootstrapper, commandLineConfig(
                "-c", configOption( EnterpriseServerSettings.mode.name(), "HA" ),
                "-c", configOption( ClusterSettings.server_id.name(), "1" ),
                "-c", configOption( ClusterSettings.initial_hosts.name(), "127.0.0.1:5001" ),
                "-c", configOption( data_directory.name(), getRelativePath( folder.getRoot(), data_directory ) ),
                "-c", configOption( auth_store.name(), getRelativePath( folder.getRoot(), auth_store ) ),
                "-c", configOption( tls_key_file.name(), getRelativePath( folder.getRoot(), tls_key_file ) ),
                "-c", configOption( tls_certificate_file.name(), getRelativePath( folder.getRoot(), tls_certificate_file ) )
        ));

        // Then
        assertEquals( Bootstrapper.OK, resultCode );
        assertNotNull( bootstrapper.getServer() );
    }
}

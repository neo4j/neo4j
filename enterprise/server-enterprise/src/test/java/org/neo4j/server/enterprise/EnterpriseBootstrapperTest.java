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

import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.server.BaseBootstrapper;
import org.neo4j.server.BaseBootstrapperTest;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;

import static org.neo4j.dbms.DatabaseManagementSystemSettings.data_directory;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.auth_store;
import static org.neo4j.server.ServerTestUtils.getRelativePath;
import static org.neo4j.server.configuration.ServerSettings.tls_certificate_file;
import static org.neo4j.server.configuration.ServerSettings.tls_key_file;
import static org.neo4j.test.Assert.assertEventually;

public class EnterpriseBootstrapperTest extends BaseBootstrapperTest
{
    @Override
    protected BaseBootstrapper newBootstrapper()
    {
        return new EnterpriseBootstrapper();
    }

    @Override
    protected void start(String[] args)
    {
        EnterpriseBootstrapper.start( args );
    }

    @Override
    protected void stop(String[] args)
    {
        EnterpriseBootstrapper.stop( args );
    }

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void shouldBeAbleToStartInSingleMode() throws Exception
    {
        // When
        int resultCode = BaseBootstrapper.start( bootstrapper,
                "-c", configOption( EnterpriseServerSettings.mode, "SINGLE" ),
                "-c", configOption( data_directory, getRelativePath( folder.getRoot(), data_directory ) ),
                "-c", configOption( auth_store, getRelativePath( folder.getRoot(), auth_store ) ),
                "-c", configOption( tls_key_file, getRelativePath( folder.getRoot(), tls_key_file ) ),
                "-c", configOption( tls_certificate_file, getRelativePath( folder.getRoot(), tls_certificate_file ) ) );

        // Then
        assertEquals( BaseBootstrapper.OK, resultCode );
        assertEventually( "Server was not started", bootstrapper::isRunning, is( true ), 1, TimeUnit.MINUTES );
    }

    @Test
    public void shouldBeAbleToStartInHAMode() throws Exception
    {
        // When
        int resultCode = BaseBootstrapper.start( bootstrapper,
                "-c", configOption( EnterpriseServerSettings.mode, "HA" ),
                "-c", configOption( ClusterSettings.server_id, "1" ),
                "-c", configOption( ClusterSettings.initial_hosts, "127.0.0.1:5001" ),
                "-c", configOption( data_directory, getRelativePath( folder.getRoot(), data_directory ) ),
                "-c", configOption( auth_store, getRelativePath( folder.getRoot(), auth_store ) ),
                "-c", configOption( tls_key_file, getRelativePath( folder.getRoot(), tls_key_file ) ),
                "-c", configOption( tls_certificate_file, getRelativePath( folder.getRoot(), tls_certificate_file ) ) );

        // Then
        assertEquals( BaseBootstrapper.OK, resultCode );
        assertEventually( "Server was not started", bootstrapper::isRunning, is( true ), 1, TimeUnit.MINUTES );
    }
}

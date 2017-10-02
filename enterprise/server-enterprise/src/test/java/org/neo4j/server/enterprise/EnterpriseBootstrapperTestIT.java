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
package org.neo4j.server.enterprise;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.com.ports.allocation.PortAuthority;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.BaseBootstrapperTestIT;
import org.neo4j.server.NeoServer;
import org.neo4j.server.ServerBootstrapper;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.test.rule.CleanupRule;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.dbms.DatabaseManagementSystemSettings.data_directory;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.logs_directory;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_internal_log_level;
import static org.neo4j.helpers.collection.MapUtil.store;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.configuration.ssl.LegacySslPolicyConfig.certificates_directory;
import static org.neo4j.server.ServerTestUtils.getRelativePath;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class EnterpriseBootstrapperTestIT extends BaseBootstrapperTestIT
{
    private final TemporaryFolder folder = new TemporaryFolder();
    private final CleanupRule cleanupRule = new CleanupRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule(folder).around( cleanupRule );

    @Override
    protected ServerBootstrapper newBootstrapper()
    {
        return new EnterpriseBootstrapper();
    }

    @Test
    public void shouldBeAbleToStartInSingleMode() throws Exception
    {
        // When
        int resultCode = ServerBootstrapper.start( bootstrapper,
                "--home-dir", tempDir.newFolder( "home-dir" ).getAbsolutePath(),
                "-c", configOption( EnterpriseEditionSettings.mode, "SINGLE" ),
                "-c", configOption( data_directory, getRelativePath( folder.getRoot(), data_directory ) ),
                "-c", configOption( logs_directory, tempDir.getRoot().getAbsolutePath() ),
                "-c", configOption( certificates_directory, getRelativePath( folder.getRoot(), certificates_directory ) ),
                "-c", configOption( OnlineBackupSettings.online_backup_server, "127.0.0.1:" + PortAuthority.allocatePort() ),
                "-c", "dbms.connector.1.type=HTTP",
                "-c", "dbms.connector.1.encryption=NONE",
                "-c", "dbms.connector.1.enabled=true" );

        // Then
        assertEquals( ServerBootstrapper.OK, resultCode );
        assertEventually( "Server was not started", bootstrapper::isRunning, is( true ), 1, TimeUnit.MINUTES );
    }

    @Test
    public void shouldBeAbleToStartInHAMode() throws Exception
    {
        // When
        int clusterPort = PortAuthority.allocatePort();
        int resultCode = ServerBootstrapper.start( bootstrapper,
                "--home-dir", tempDir.newFolder( "home-dir" ).getAbsolutePath(),
                "-c", configOption( EnterpriseEditionSettings.mode, "HA" ),
                "-c", configOption( ClusterSettings.server_id, "1" ),
                "-c", configOption( ClusterSettings.initial_hosts, "127.0.0.1:" + clusterPort ),
                "-c", configOption( ClusterSettings.cluster_server, "127.0.0.1:" + clusterPort ),
                "-c", configOption( data_directory, getRelativePath( folder.getRoot(), data_directory ) ),
                "-c", configOption( logs_directory, tempDir.getRoot().getAbsolutePath() ),
                "-c", configOption( certificates_directory, getRelativePath( folder.getRoot(), certificates_directory ) ),
                "-c", configOption( OnlineBackupSettings.online_backup_server, "127.0.0.1:" + PortAuthority.allocatePort() ),
                "-c", "dbms.connector.1.type=HTTP",
                "-c", "dbms.connector.1.encryption=NONE",
                "-c", "dbms.connector.1.enabled=true" );

        // Then
        assertEquals( ServerBootstrapper.OK, resultCode );
        assertEventually( "Server was not started", bootstrapper::isRunning, is( true ), 1, TimeUnit.MINUTES );
    }

    @Test
    public void debugLoggingDisabledByDefault() throws Exception
    {
        // When
        File configFile = tempDir.newFile( Config.DEFAULT_CONFIG_FILE_NAME );

        Map<String, String> properties = stringMap();
        properties.putAll( ServerTestUtils.getDefaultRelativeProperties() );
        properties.put( "dbms.connector.1.type", "HTTP" );
        properties.put( "dbms.connector.1.encryption", "NONE" );
        properties.put( "dbms.connector.1.enabled", "true" );
        properties.put( OnlineBackupSettings.online_backup_server.name(), "127.0.0.1:" + PortAuthority.allocatePort() );
        store( properties, configFile );

        // When
        UncoveredEnterpriseBootstrapper uncoveredEnterpriseBootstrapper = new UncoveredEnterpriseBootstrapper();
        cleanupRule.add( uncoveredEnterpriseBootstrapper );
        ServerBootstrapper.start( uncoveredEnterpriseBootstrapper,
                "--home-dir", tempDir.newFolder( "home-dir" ).getAbsolutePath(),
                "--config-dir", configFile.getParentFile().getAbsolutePath() );

        // Then
        assertEventually( "Server was started", uncoveredEnterpriseBootstrapper::isRunning, is( true ), 1, TimeUnit.MINUTES );
        LogProvider userLogProvider = uncoveredEnterpriseBootstrapper.getUserLogProvider();
        assertFalse( "Debug logging is disabled by default", userLogProvider.getLog( getClass() ).isDebugEnabled() );
    }

    @Test
    public void debugLoggingEnabledBySetting() throws Exception
    {
        // When
        File configFile = tempDir.newFile( Config.DEFAULT_CONFIG_FILE_NAME );

        Map<String, String> properties = stringMap( store_internal_log_level.name(), "DEBUG");
        properties.putAll( ServerTestUtils.getDefaultRelativeProperties() );
        properties.put( "dbms.connector.1.type", "HTTP" );
        properties.put( "dbms.connector.1.encryption", "NONE" );
        properties.put( "dbms.connector.1.enabled", "true" );
        properties.put( OnlineBackupSettings.online_backup_server.name(), "127.0.0.1:" + PortAuthority.allocatePort() );
        store( properties, configFile );

        // When
        UncoveredEnterpriseBootstrapper uncoveredEnterpriseBootstrapper = new UncoveredEnterpriseBootstrapper();
        cleanupRule.add( uncoveredEnterpriseBootstrapper );
        ServerBootstrapper.start( uncoveredEnterpriseBootstrapper,
                "--home-dir", tempDir.newFolder( "home-dir" ).getAbsolutePath(),
                "--config-dir", configFile.getParentFile().getAbsolutePath() );

        // Then
        assertEventually( "Server was started", uncoveredEnterpriseBootstrapper::isRunning, is( true ), 1, TimeUnit.MINUTES );
        LogProvider userLogProvider = uncoveredEnterpriseBootstrapper.getUserLogProvider();
        assertTrue( "Debug logging enabled by setting value.", userLogProvider.getLog( getClass() ).isDebugEnabled() );
    }

    private class UncoveredEnterpriseBootstrapper extends EnterpriseBootstrapper
    {
        private LogProvider userLogProvider;

        @Override
        protected NeoServer createNeoServer( Config configurator, GraphDatabaseDependencies dependencies,
                LogProvider userLogProvider )
        {
            this.userLogProvider = userLogProvider;
            return super.createNeoServer( configurator, dependencies, userLogProvider );
        }

        LogProvider getUserLogProvider()
        {
            return userLogProvider;
        }
    }
}

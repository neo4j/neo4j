/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.server.enterprise;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.BaseBootstrapperTest;
import org.neo4j.server.NeoServer;
import org.neo4j.server.ServerBootstrapper;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.server.configuration.ConfigLoader;
import org.neo4j.test.rule.CleanupRule;
import org.neo4j.test.rule.TestDirectory;

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
import static org.neo4j.server.configuration.ServerSettings.script_enabled;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class EnterpriseBootstrapperTest extends BaseBootstrapperTest
{
    private TemporaryFolder folder = new TemporaryFolder();
    private CleanupRule cleanupRule = new CleanupRule();

    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

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
                "-c", configOption( ClusterSettings.mode, "SINGLE" ),
                "-c", configOption( data_directory, getRelativePath( folder.getRoot(), data_directory ) ),
                "-c", configOption( logs_directory, tempDir.getRoot().getAbsolutePath() ),
                "-c", configOption( certificates_directory, getRelativePath( folder.getRoot(), certificates_directory ) ),
                // The `script_enabled=true` setting is needed because the global javascript context must be
                // initialised in sandboxed mode to allow testing traversal endpoint scripting:
                "-c", configOption( script_enabled, Settings.TRUE ),
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
        int resultCode = ServerBootstrapper.start( bootstrapper,
                "--home-dir", tempDir.newFolder( "home-dir" ).getAbsolutePath(),
                "-c", configOption( ClusterSettings.mode, "HA" ),
                "-c", configOption( ClusterSettings.server_id, "1" ),
                "-c", configOption( ClusterSettings.initial_hosts, "127.0.0.1:5001" ),
                "-c", configOption( data_directory, getRelativePath( folder.getRoot(), data_directory ) ),
                "-c", configOption( logs_directory, tempDir.getRoot().getAbsolutePath() ),
                "-c", configOption( certificates_directory, getRelativePath( folder.getRoot(), certificates_directory ) ),
                // The `script_enabled=true` setting is needed because the global javascript context must be
                // initialised in sandboxed mode to allow testing traversal endpoint scripting:
                "-c", configOption( script_enabled, Settings.TRUE ),
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
        File configFile = tempDir.newFile( ConfigLoader.DEFAULT_CONFIG_FILE_NAME );

        Map<String, String> properties = stringMap();
        properties.putAll( ServerTestUtils.getDefaultRelativeProperties() );
        properties.put( "dbms.connector.1.type", "HTTP" );
        properties.put( "dbms.connector.1.encryption", "NONE" );
        properties.put( "dbms.connector.1.enabled", "true" );
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
        File configFile = tempDir.newFile( ConfigLoader.DEFAULT_CONFIG_FILE_NAME );

        Map<String, String> properties = stringMap( store_internal_log_level.name(), "DEBUG");
        properties.putAll( ServerTestUtils.getDefaultRelativeProperties() );
        properties.put( "dbms.connector.1.type", "HTTP" );
        properties.put( "dbms.connector.1.encryption", "NONE" );
        properties.put( "dbms.connector.1.enabled", "true" );
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

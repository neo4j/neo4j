/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.configuration;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.test.rule.SuppressOutput;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.neo4j.test.rule.SuppressOutput.suppressAll;

public class ConfigLoaderTest
{
    @Rule
    public final SuppressOutput suppressOutput = suppressAll();
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void shouldProvideAConfiguration()
    {
        // given
        File configFile = ConfigFileBuilder.builder( folder.getRoot() ).build();

        // when
        Config config = Config.fromFile( configFile ).withHome( folder.getRoot() ).build();

        // then
        assertNotNull( config );
    }

    @Test
    public void shouldUseSpecifiedConfigFile()
    {
        // given
        File configFile = ConfigFileBuilder.builder( folder.getRoot() )
                .withNameValue( GraphDatabaseSettings.default_advertised_address.name(), "bar" )
                .build();

        // when
        Config testConf = Config.fromFile( configFile ).withHome( folder.getRoot() ).build();

        // then
        final String EXPECTED_VALUE = "bar";
        assertEquals( EXPECTED_VALUE, testConf.get( GraphDatabaseSettings.default_advertised_address ) );
    }

    @Test
    public void shouldUseSpecifiedHomeDir()
    {
        // given
        File configFile = ConfigFileBuilder.builder( folder.getRoot() ).build();

        // when
        Config testConf = Config.fromFile( configFile ).withHome( folder.getRoot() ).build();

        // then
        assertEquals( folder.getRoot(), testConf.get( GraphDatabaseSettings.neo4j_home ) );
    }

    @Test
    public void shouldUseWorkingDirForHomeDirIfUnspecified()
    {
        // given
        File configFile = ConfigFileBuilder.builder( folder.getRoot() ).build();

        // when
        Config testConf = Config.fromFile( configFile ).build();

        // then
        assertEquals( new File( System.getProperty("user.dir") ),
                testConf.get( GraphDatabaseSettings.neo4j_home ) );
    }

    @Test
    public void shouldAcceptDuplicateKeysWithSameValue()
    {
        // given
        File configFile = ConfigFileBuilder.builder( folder.getRoot() )
                .withNameValue( GraphDatabaseSettings.default_advertised_address.name(), "bar" )
                .withNameValue( GraphDatabaseSettings.default_advertised_address.name(), "bar" )
                .build();

        // when
        Config testConf = Config.fromFile( configFile ).withHome( folder.getRoot() ).build();

        // then
        assertNotNull( testConf );
        final String EXPECTED_VALUE = "bar";
        assertEquals( EXPECTED_VALUE, testConf.get( GraphDatabaseSettings.default_advertised_address ) );
    }

    @Test
    public void loadOfflineConfigShouldDisableBolt()
    {
        // given
        BoltConnector defaultBoltConf = new BoltConnector( "bolt" );
        File configFile = ConfigFileBuilder.builder( folder.getRoot() )
                .withNameValue( defaultBoltConf.enabled.name(), Settings.TRUE )
                .build();

        // when
        Config testConf = Config.fromFile( configFile ).withHome( folder.getRoot() ).withConnectorsDisabled().build();

        // then
        assertNotNull( testConf );
        assertEquals( false, testConf.get( defaultBoltConf.enabled ) );
        assertEquals( false, testConf.get( new BoltConnector().enabled ) );
    }

    @Test
    public void loadOfflineConfigAddDisabledBoltConnector()
    {
        // given
        File configFile = ConfigFileBuilder.builder( folder.getRoot() ).build();

        // when
        Config testConf = Config.fromFile( configFile ).withHome( folder.getRoot() ).withConnectorsDisabled().build();

        // then
        assertNotNull( testConf );
        assertEquals( false, testConf.get( new BoltConnector().enabled ) );
    }

    @Test
    public void shouldFindThirdPartyJaxRsPackages() throws IOException
    {
        // given
        File file = ServerTestUtils.createTempConfigFile( folder.getRoot() );

        try ( BufferedWriter out = new BufferedWriter( new FileWriter( file, true ) ) )
        {
            out.write( ServerSettings.third_party_packages.name() );
            out.write( "=" );
            out.write( "com.foo.bar=\"mount/point/foo\"," );
            out.write( "com.foo.baz=\"/bar\"," );
            out.write( "com.foo.foobarbaz=\"/\"" );
            out.write( System.lineSeparator() );
        }

        // when
        Config config = Config.fromFile( file ).withHome( folder.getRoot() ).build();

        // then
        List<ThirdPartyJaxRsPackage> thirdpartyJaxRsPackages = config.get( ServerSettings.third_party_packages );
        assertNotNull( thirdpartyJaxRsPackages );
        assertEquals( 3, thirdpartyJaxRsPackages.size() );
    }

    @Test
    public void shouldRetainRegistrationOrderOfThirdPartyJaxRsPackages()
    {
        // given
        File configFile = ConfigFileBuilder.builder( folder.getRoot() )
                .withNameValue( ServerSettings.third_party_packages.name(),
                        "org.neo4j.extension.extension1=/extension1,org.neo4j.extension.extension2=/extension2," +
                        "org.neo4j.extension.extension3=/extension3" )
                .build();

        // when
        Config config = Config.fromFile( configFile ).withHome( folder.getRoot() ).build();

        // then
        List<ThirdPartyJaxRsPackage> thirdpartyJaxRsPackages = config.get( ServerSettings.third_party_packages );

        assertEquals( 3, thirdpartyJaxRsPackages.size() );
        assertEquals( "/extension1", thirdpartyJaxRsPackages.get( 0 ).getMountPoint() );
        assertEquals( "/extension2", thirdpartyJaxRsPackages.get( 1 ).getMountPoint() );
        assertEquals( "/extension3", thirdpartyJaxRsPackages.get( 2 ).getMountPoint() );

    }

    @Test
    public void shouldWorkFineWhenSpecifiedConfigFileDoesNotExist()
    {
        // Given
        File nonExistentConfigFile = new File( "/tmp/" + System.currentTimeMillis() );

        // When
        Config config = Config.fromFile( nonExistentConfigFile ).withHome( folder.getRoot() ).build();

        // Then
        assertNotNull( config );
    }

    @Test
    public void shouldDefaultToCorrectValueForAuthStoreLocation()
    {
        File configFile = ConfigFileBuilder
                .builder( folder.getRoot() )
                .withoutSetting( GraphDatabaseSettings.data_directory )
                .build();
        Config config = Config.fromFile( configFile ).withHome( folder.getRoot() ).build();

        assertThat( config.get( DatabaseManagementSystemSettings.auth_store_directory ),
                is( new File( folder.getRoot(), "data/dbms" ).getAbsoluteFile() ) );
    }

    @Test
    public void shouldSetAValueForAuthStoreLocation()
    {
        File configFile = ConfigFileBuilder.builder( folder.getRoot() )
                .withSetting( GraphDatabaseSettings.data_directory, "the-data-dir" )
                .build();
        Config config = Config.fromFile( configFile ).withHome( folder.getRoot() ).build();

        assertThat( config.get( DatabaseManagementSystemSettings.auth_store_directory ),
                is( new File( folder.getRoot(), "the-data-dir/dbms" ).getAbsoluteFile() ) );
    }

    @Test
    public void shouldNotOverwriteAuthStoreLocationIfProvided()
    {
        File configFile = ConfigFileBuilder.builder( folder.getRoot() )
                .withSetting( GraphDatabaseSettings.data_directory, "the-data-dir" )
                .withSetting( GraphDatabaseSettings.auth_store, "foo/bar/auth" )
                .build();
        Config config = Config.fromFile( configFile ).withHome( folder.getRoot() ).build();

        assertThat( config.get( GraphDatabaseSettings.auth_store ),
                is( new File( folder.getRoot(), "foo/bar/auth" ).getAbsoluteFile() ) );
    }
}

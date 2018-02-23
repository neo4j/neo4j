/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import javax.annotation.Resource;

import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith( {SuppressOutputExtension.class, TestDirectoryExtension.class} )
public class ConfigLoaderTest
{
    @Resource
    public SuppressOutput suppressOutput;
    @Resource
    public TestDirectory folder;

    @Test
    public void shouldProvideAConfiguration()
    {
        // given
        File configFile = ConfigFileBuilder.builder( folder.directory() ).build();

        // when
        Config config = Config.fromFile( configFile ).withHome( folder.directory() ).build();

        // then
        assertNotNull( config );
    }

    @Test
    public void shouldUseSpecifiedConfigFile()
    {
        // given
        File configFile = ConfigFileBuilder.builder( folder.directory() )
                .withNameValue( GraphDatabaseSettings.default_advertised_address.name(), "bar" )
                .build();

        // when
        Config testConf = Config.fromFile( configFile ).withHome( folder.directory() ).build();

        // then
        final String EXPECTED_VALUE = "bar";
        assertEquals( EXPECTED_VALUE, testConf.get( GraphDatabaseSettings.default_advertised_address ) );
    }

    @Test
    public void shouldUseSpecifiedHomeDir()
    {
        // given
        File configFile = ConfigFileBuilder.builder( folder.directory() ).build();

        // when
        Config testConf = Config.fromFile( configFile ).withHome( folder.directory() ).build();

        // then
        assertEquals( folder.directory(), testConf.get( GraphDatabaseSettings.neo4j_home ) );
    }

    @Test
    public void shouldUseWorkingDirForHomeDirIfUnspecified()
    {
        // given
        File configFile = ConfigFileBuilder.builder( folder.directory() ).build();

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
        File configFile = ConfigFileBuilder.builder( folder.directory() )
                .withNameValue( GraphDatabaseSettings.default_advertised_address.name(), "bar" )
                .withNameValue( GraphDatabaseSettings.default_advertised_address.name(), "bar" )
                .build();

        // when
        Config testConf = Config.fromFile( configFile ).withHome( folder.directory() ).build();

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
        File configFile = ConfigFileBuilder.builder( folder.directory() )
                .withNameValue( defaultBoltConf.enabled.name(), Settings.TRUE )
                .build();

        // when
        Config testConf = Config.fromFile( configFile ).withHome( folder.directory() ).withConnectorsDisabled().build();

        // then
        assertNotNull( testConf );
        assertEquals( false, testConf.get( defaultBoltConf.enabled ) );
        assertEquals( false, testConf.get( new BoltConnector().enabled ) );
    }

    @Test
    public void loadOfflineConfigAddDisabledBoltConnector()
    {
        // given
        File configFile = ConfigFileBuilder.builder( folder.directory() ).build();

        // when
        Config testConf = Config.fromFile( configFile ).withHome( folder.directory() ).withConnectorsDisabled().build();

        // then
        assertNotNull( testConf );
        assertEquals( false, testConf.get( new BoltConnector().enabled ) );
    }

    @Test
    public void shouldFindThirdPartyJaxRsPackages() throws IOException
    {
        // given
        File file = ServerTestUtils.createTempConfigFile( folder.directory() );

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
        Config config = Config.fromFile( file ).withHome( folder.directory() ).build();

        // then
        List<ThirdPartyJaxRsPackage> thirdpartyJaxRsPackages = config.get( ServerSettings.third_party_packages );
        assertNotNull( thirdpartyJaxRsPackages );
        assertEquals( 3, thirdpartyJaxRsPackages.size() );
    }

    @Test
    public void shouldRetainRegistrationOrderOfThirdPartyJaxRsPackages()
    {
        // given
        File configFile = ConfigFileBuilder.builder( folder.directory() )
                .withNameValue( ServerSettings.third_party_packages.name(),
                        "org.neo4j.extension.extension1=/extension1,org.neo4j.extension.extension2=/extension2," +
                        "org.neo4j.extension.extension3=/extension3" )
                .build();

        // when
        Config config = Config.fromFile( configFile ).withHome( folder.directory() ).build();

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
        Config config = Config.fromFile( nonExistentConfigFile ).withHome( folder.directory() ).build();

        // Then
        assertNotNull( config );
    }

    @Test
    public void shouldDefaultToCorrectValueForAuthStoreLocation()
    {
        File configFile = ConfigFileBuilder
                .builder( folder.directory() )
                .withoutSetting( GraphDatabaseSettings.data_directory )
                .build();
        Config config = Config.fromFile( configFile ).withHome( folder.directory() ).build();

        assertThat( config.get( DatabaseManagementSystemSettings.auth_store_directory ),
                is( new File( folder.directory(), "data/dbms" ).getAbsoluteFile() ) );
    }

    @Test
    public void shouldSetAValueForAuthStoreLocation()
    {
        File configFile = ConfigFileBuilder.builder( folder.directory() )
                .withSetting( GraphDatabaseSettings.data_directory, "the-data-dir" )
                .build();
        Config config = Config.fromFile( configFile ).withHome( folder.directory() ).build();

        assertThat( config.get( DatabaseManagementSystemSettings.auth_store_directory ),
                is( new File( folder.directory(), "the-data-dir/dbms" ).getAbsoluteFile() ) );
    }

    @Test
    public void shouldNotOverwriteAuthStoreLocationIfProvided()
    {
        File configFile = ConfigFileBuilder.builder( folder.directory() )
                .withSetting( GraphDatabaseSettings.data_directory, "the-data-dir" )
                .withSetting( GraphDatabaseSettings.auth_store, "foo/bar/auth" )
                .build();
        Config config = Config.fromFile( configFile ).withHome( folder.directory() ).build();

        assertThat( config.get( GraphDatabaseSettings.auth_store ),
                is( new File( folder.directory(), "foo/bar/auth" ).getAbsoluteFile() ) );
    }
}

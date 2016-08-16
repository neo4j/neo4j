/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.CommunityBootstrapper;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.test.rule.SuppressOutput;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.neo4j.kernel.configuration.Settings.NO_DEFAULT;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.setting;
import static org.neo4j.test.rule.SuppressOutput.suppressAll;

public class ConfigLoaderTest
{
    @Rule
    public final SuppressOutput suppressOutput = suppressAll();
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    private final ConfigLoader configLoader = new ConfigLoader( CommunityBootstrapper.settingsClasses );

    @Test
    public void shouldProvideAConfiguration() throws IOException
    {
        // given
        Optional<File> configFile = ConfigFileBuilder.builder( folder.getRoot() )
                .build();

        // when
        Config config = configLoader.loadConfig( Optional.of( folder.getRoot() ), configFile );

        // then
        assertNotNull( config );
    }

    @Test
    public void shouldUseSpecifiedConfigFile() throws Exception
    {
        // given
        Optional<File> configFile = ConfigFileBuilder.builder( folder.getRoot() )
                .withNameValue( "foo", "bar" )
                .build();

        // when
        Config testConf = configLoader.loadConfig( Optional.of( folder.getRoot() ), configFile );

        // then
        final String EXPECTED_VALUE = "bar";
        assertEquals( EXPECTED_VALUE, testConf.get( setting( "foo", STRING, NO_DEFAULT ) ) );
    }

    @Test
    public void shouldUseSpecifiedHomeDir() throws Exception
    {
        // given
        Optional<File> configFile = ConfigFileBuilder.builder( folder.getRoot() )
                .build();

        // when
        Config testConf = configLoader.loadConfig( Optional.of( folder.getRoot() ), configFile );

        // then
        assertEquals( folder.getRoot(), testConf.get( GraphDatabaseSettings.neo4j_home ) );
    }

    @Test
    public void shouldUseWorkingDirForHomeDirIfUnspecified() throws Exception
    {
        // given
        Optional<File> configFile = ConfigFileBuilder.builder( folder.getRoot() )
                .build();

        // when
        Config testConf = configLoader.loadConfig( Optional.empty(), configFile );

        // then
        assertEquals( new File( System.getProperty("user.dir") ),
                testConf.get( GraphDatabaseSettings.neo4j_home ) );
    }

    @Test
    public void shouldAcceptDuplicateKeysWithSameValue() throws IOException
    {
        // given
        Optional<File> configFile = ConfigFileBuilder.builder( folder.getRoot() )
                .withNameValue( "foo", "bar" )
                .withNameValue( "foo", "bar" )
                .build();

        // when
        Config testConf = configLoader.loadConfig( Optional.of( folder.getRoot() ), configFile );

        // then
        assertNotNull( testConf );
        final String EXPECTED_VALUE = "bar";
        assertEquals( EXPECTED_VALUE, testConf.get( setting( "foo", STRING, NO_DEFAULT ) ) );
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
        Config config = configLoader.loadConfig( Optional.of( folder.getRoot() ), Optional.of( file ) );

        // then
        List<ThirdPartyJaxRsPackage> thirdpartyJaxRsPackages = config.get( ServerSettings.third_party_packages );
        assertNotNull( thirdpartyJaxRsPackages );
        assertEquals( 3, thirdpartyJaxRsPackages.size() );
    }

    @Test
    public void shouldRetainRegistrationOrderOfThirdPartyJaxRsPackages() throws IOException
    {
        // given
        Optional<File> configFile = ConfigFileBuilder.builder( folder.getRoot() )
                .withNameValue( ServerSettings.third_party_packages.name(),
                        "org.neo4j.extension.extension1=/extension1,org.neo4j.extension.extension2=/extension2," +
                        "org.neo4j.extension.extension3=/extension3" )
                .build();

        // when
        Config config = configLoader.loadConfig( Optional.of( folder.getRoot() ), configFile );

        // then
        List<ThirdPartyJaxRsPackage> thirdpartyJaxRsPackages = config.get( ServerSettings.third_party_packages );

        assertEquals( 3, thirdpartyJaxRsPackages.size() );
        assertEquals( "/extension1", thirdpartyJaxRsPackages.get( 0 ).getMountPoint() );
        assertEquals( "/extension2", thirdpartyJaxRsPackages.get( 1 ).getMountPoint() );
        assertEquals( "/extension3", thirdpartyJaxRsPackages.get( 2 ).getMountPoint() );

    }

    @Test
    public void shouldWorkFineWhenSpecifiedConfigFileDoesNotExist() throws IOException
    {
        // Given
        Optional<File> nonExistentConfigFile = Optional.of( new File( "/tmp/" + System.currentTimeMillis() ) );

        // When
        Config config = configLoader.loadConfig( Optional.of( folder.getRoot() ), nonExistentConfigFile );

        // Then
        assertNotNull( config );
    }

    @Test
    public void shouldDefaultToCorrectValueForAuthStoreLocation() throws IOException
    {
        Optional<File> configFile = ConfigFileBuilder
                .builder( folder.getRoot() )
                .withoutSetting( DatabaseManagementSystemSettings.data_directory )
                .build();
        Config config = configLoader.loadConfig( Optional.of( folder.getRoot() ), configFile );

        assertThat( config.get( DatabaseManagementSystemSettings.auth_store_directory ),
                is( new File( folder.getRoot(), "data/dbms" ).getAbsoluteFile() ) );
    }

    @Test
    public void shouldSetAValueForAuthStoreLocation() throws IOException
    {
        Optional<File> configFile = ConfigFileBuilder.builder( folder.getRoot() )
                .withSetting( DatabaseManagementSystemSettings.data_directory, "the-data-dir" )
                .build();
        Config config = configLoader.loadConfig( Optional.of( folder.getRoot() ), configFile );

        assertThat( config.get( DatabaseManagementSystemSettings.auth_store_directory ),
                is( new File( folder.getRoot(), "the-data-dir/dbms" ).getAbsoluteFile() ) );
    }

    @Test
    public void shouldNotOverwriteAuthStoreLocationIfProvided() throws IOException
    {
        Optional<File> configFile = ConfigFileBuilder.builder( folder.getRoot() )
                .withSetting( DatabaseManagementSystemSettings.data_directory, "the-data-dir" )
                .withSetting( GraphDatabaseSettings.auth_store, "foo/bar/auth" )
                .build();
        Config config = configLoader.loadConfig( Optional.of( folder.getRoot() ), configFile );

        assertThat( config.get( GraphDatabaseSettings.auth_store ),
                is( new File( folder.getRoot(), "foo/bar/auth" ).getAbsoluteFile() ) );
    }
}

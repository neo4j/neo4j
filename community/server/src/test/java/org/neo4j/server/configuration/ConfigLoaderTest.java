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
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.neo4j.server.CommunityBootstrapper;
import org.neo4j.server.ServerCommandLineArgs;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.test.SuppressOutput;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.helpers.ArrayUtil.array;
import static org.neo4j.kernel.configuration.Settings.NO_DEFAULT;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.setting;
import static org.neo4j.test.SuppressOutput.suppressAll;

public class ConfigLoaderTest
{
    @Rule
    public final SuppressOutput suppressOutput = suppressAll();
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    private final Log log = NullLog.getInstance();
    private final ConfigLoader configLoader = new ConfigLoader( CommunityBootstrapper.settingsClasses );

    @Test
    public void shouldProvideAConfiguration() throws IOException
    {
        // given
        Optional<File> configFile = ConfigFileBuilder.builder( folder.getRoot() )
                .build();

        // when
        Config config = configLoader.loadConfig( configFile, log );

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
        Config testConf = configLoader.loadConfig( configFile, log );

        // then
        final String EXPECTED_VALUE = "bar";
        assertEquals( EXPECTED_VALUE, testConf.get( setting( "foo", STRING, NO_DEFAULT ) ) );
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
        Config testConf = configLoader.loadConfig( configFile, log );

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
        Config config = configLoader.loadConfig( Optional.of( file ), log );

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
        Config config = configLoader.loadConfig( configFile, log );

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
        Optional<File> nonExistentConfigFile = Optional.of( new File( "/tmp/" + System.currentTimeMillis() ) );

        // When
        Config config = configLoader.loadConfig( nonExistentConfigFile, log );

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
        Config config = configLoader.loadConfig( configFile, log );

        assertThat( config.get( GraphDatabaseSettings.auth_store ),
                is( new File( "data/dbms/auth" ).getAbsoluteFile() ) );
    }

    @Test
    public void shouldSetAValueForAuthStoreLocation() throws IOException
    {
        Optional<File> configFile = ConfigFileBuilder.builder( folder.getRoot() )
                .withSetting( DatabaseManagementSystemSettings.data_directory, "the-data-dir" )
                .build();
        Config config = configLoader.loadConfig( configFile, log );

        assertThat( config.get( GraphDatabaseSettings.auth_store ),
                is( new File( "the-data-dir/dbms/auth" ).getAbsoluteFile() ) );
    }

    @Test
    public void shouldNotOverwriteAuthStoreLocationIfProvided() throws IOException
    {
        Optional<File> configFile = ConfigFileBuilder.builder( folder.getRoot() )
                .withSetting( DatabaseManagementSystemSettings.data_directory, "the-data-dir" )
                .withSetting( GraphDatabaseSettings.auth_store, "foo/bar/auth" )
                .build();
        Config config = configLoader.loadConfig( configFile, log );

        assertThat( config.get( GraphDatabaseSettings.auth_store ),
                is( new File( "foo/bar/auth" ).getAbsoluteFile() ) );
    }

    @Test
    public void shouldWarnForNonExistingSetting() throws IOException
    {
        // given
        File file = ServerTestUtils.createTempConfigFile( folder.getRoot() );
        Log mockLog = mock(Log.class);

        try ( BufferedWriter out = new BufferedWriter( new FileWriter( file, true ) ) )
        {
            out.write( "this.setting.does.not.exist=value" );
            out.write( System.lineSeparator() );
        }

        // when
        Config config = configLoader.loadConfig( Optional.of( file ), mockLog );

        // then
        verify( mockLog ).warn( "The setting 'this.setting.does.not.exist' is not recognized and will not have any effect." );
    }

    @Test
    public void shouldNotWarnForExistingSetting() throws IOException
    {
        // given
        File file = ServerTestUtils.createTempConfigFile( folder.getRoot() );
        Log mockLog = mock(Log.class);

        try ( BufferedWriter out = new BufferedWriter( new FileWriter( file, true ) ) )
        {
            out.write( GraphDatabaseSettings.logs_directory.name() + "=/tmp/log" );
            out.write( System.lineSeparator() );
        }

        // when
        Config config = configLoader.loadConfig( Optional.of( file ), mockLog );

        // then
        verify( mockLog, times(0) ).warn( "The setting '" + GraphDatabaseSettings.logs_directory.name() +
                                          "' is not recognized and will not have any effect." );
    }

    @Test
    public void shouldWarnForNonExistingCommandLineSetting() throws IOException
    {
        // Given
        File file = ServerTestUtils.createTempConfigFile( folder.getRoot() );
        String[] args = array(
                "-c", "this.setting.does.also.not.exist=value" );
        ServerCommandLineArgs parsed = ServerCommandLineArgs.parse( args );

        Log mockLog = mock(Log.class);

        // When
        Config config = configLoader.loadConfig( Optional.of( file ), mockLog, parsed.configOverrides() );

        // then
        verify( mockLog ).warn( "The setting 'this.setting.does.also.not.exist' is not recognized and will not have any effect." );
    }

    @Test
    public void shouldNotWarnForExistingCommandLineSetting() throws IOException
    {
        // Given
        File file = ServerTestUtils.createTempConfigFile( folder.getRoot() );
        String[] args = array(
                "-c", ServerSettings.http_logging_enabled.name() + "=true" );
        ServerCommandLineArgs parsed = ServerCommandLineArgs.parse( args );

        Log mockLog = mock(Log.class);

        // When
        Config config = configLoader.loadConfig( Optional.of( file ), mockLog, parsed.configOverrides() );

        // then
        verify( mockLog, times(0) ).warn( "The setting '" + ServerSettings.http_logging_enabled.name() +
                                          "' is not recognized and will not have any effect." );
    }
}

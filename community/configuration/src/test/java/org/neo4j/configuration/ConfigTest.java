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
package org.neo4j.configuration;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.configuration.SettingValueParsers.INT;
import static org.neo4j.configuration.SettingValueParsers.PATH;
import static org.neo4j.configuration.SettingValueParsers.STRING;
import static org.neo4j.configuration.SettingValueParsers.listOf;
import static org.neo4j.logging.AssertableLogProvider.inLog;

@ExtendWith( TestDirectoryExtension.class )
class ConfigTest
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void testLoadSettingsToConfig()
    {
        Config config = Config.newBuilder().addSettingsClass( TestSettings.class ).build();
        assertEquals( "hello", config.get( TestSettings.stringSetting ) );
        assertEquals( 1, config.get( TestSettings.intSetting ) );
        assertNull( config.get( TestSettings.boolSetting ) );
    }

    @Test
    void testFetchAbsentSetting()
    {
        Config config = Config.newBuilder().addSettingsClass( TestSettings.class ).build();
        Setting<Boolean> absentSetting = newBuilder( "test.absent.bool", BOOL, null ).build();
        assertThrows( IllegalArgumentException.class, () -> config.get( absentSetting ) );
    }

    @Test
    void testUpdateValue()
    {
        Config config = Config.newBuilder()
                .addSettingsClass( TestSettings.class )
                .set( TestSettings.intSetting, 3 )
                .build();
        assertEquals( 3, config.get( TestSettings.intSetting ) );
        config.setDynamic( TestSettings.intSetting, 2, getClass().getSimpleName() );
        assertEquals( 2, config.get( TestSettings.intSetting ) );
        config.setDynamic( TestSettings.intSetting, null, getClass().getSimpleName() );
        assertEquals( 1, config.get( TestSettings.intSetting ) );
    }

    @Test
    void testOverrideAbsentSetting()
    {
        Map<String,String> settings = Map.of( "test.absent.bool", FALSE );
        Config.Builder builder = Config.newBuilder()
                .set( GraphDatabaseSettings.strict_config_validation, true )
                .addSettingsClass( TestSettings.class )
                .setRaw( settings );
        assertThrows( IllegalArgumentException.class, builder::build );
    }

    @Test
    void testOverrideDefault()
    {

        Map<Setting<?>,Object> overriddenDefaults =
                Map.of( TestSettings.stringSetting, "foo",
                        TestSettings.intSetting, 11,
                        TestSettings.boolSetting, true );

        Config config = Config.newBuilder()
                .addSettingsClass( TestSettings.class )
                .setDefaults( overriddenDefaults )
                .build();

        assertEquals( "foo", config.get( TestSettings.stringSetting ) );
        assertEquals( 11, config.get( TestSettings.intSetting ) );
        assertEquals( true, config.get( TestSettings.boolSetting ) );
    }

    @Test
    void testUpdateStatic()
    {
        Config config = Config.newBuilder().addSettingsClass( TestSettings.class ).build();
        assertThrows( IllegalArgumentException.class, () -> config.setDynamic( TestSettings.stringSetting, "not allowed", getClass().getSimpleName() ) );
        assertEquals( "hello", config.get( TestSettings.stringSetting ) );
        config.set( TestSettings.stringSetting, "allowed internally" );
        assertEquals( "allowed internally", config.get( TestSettings.stringSetting ) );
    }

    @Test
    void testUpdateImmutable()
    {
        Config config = Config.newBuilder().addSettingsClass( TestSettings.class ).build();
        assertThrows( IllegalArgumentException.class, () -> config.setDynamic( TestSettings.boolSetting, true, getClass().getSimpleName() ) );
        assertThrows( IllegalArgumentException.class, () -> config.set( TestSettings.boolSetting, true ) );
    }

    @Test
    void testObserver()
    {
        Config config = Config.newBuilder().addSettingsClass( TestSettings.class ).build();

        MutableInt observedOld = new MutableInt( 0 );
        MutableInt observedNew = new MutableInt( 0 );
        SettingChangeListener<Integer> listener = ( oldValue, newValue ) ->
        {
            observedOld.setValue( oldValue );
            observedNew.setValue( newValue );
        };

        config.addListener( TestSettings.intSetting, listener );

        assertEquals( 0, observedOld.getValue() );
        assertEquals( 0, observedNew.getValue() );

        config.setDynamic( TestSettings.intSetting, 2, getClass().getSimpleName() );
        assertEquals( 1, observedOld.getValue() );
        assertEquals( 2, observedNew.getValue() );

        config.setDynamic( TestSettings.intSetting, 7, getClass().getSimpleName() );
        assertEquals( 2, observedOld.getValue() );
        assertEquals( 7, observedNew.getValue() );

        config.removeListener( TestSettings.intSetting, listener );

        config.setDynamic( TestSettings.intSetting, 9, getClass().getSimpleName() );
        assertEquals( 2, observedOld.getValue() );
        assertEquals( 7, observedNew.getValue() );

        assertThrows( IllegalArgumentException.class, () -> config.addListener( TestSettings.boolSetting, ( oV, nV ) -> {} ) );
    }

    @Test
    void testGroup()
    {
        var g1 = TestConnectionGroupSetting.group( "1" );
        var g2 = TestConnectionGroupSetting.group( "2" );
        Config config = Config.newBuilder()
                .addGroupSettingClass( TestConnectionGroupSetting.class )
                .set( g1.port, 1111 )
                .set( g1.hostname, "0.0.0.0" )
                .set( g1.secure, false )
                .set( g2.port, 2222 )
                .set( g2.hostname, "127.0.0.1" )
                .build();

        assertEquals(1111, config.get( g1.port ) );
        assertEquals(2222, config.get( g2.port ) );
        assertEquals(false, config.get( g1.secure ) );
        assertEquals(true, config.get( g2.secure ) );

        assertThrows( IllegalArgumentException.class, () -> config.get( TestConnectionGroupSetting.group( "not_specified_id" ).port ) );
    }

    @Test
    void testGroupInheritance()
    {
        ChildGroup group = new ChildGroup( "1" );
        Config config = Config.newBuilder()
                .addGroupSettingClass( ChildGroup.class )
                .set( group.childSetting, "child" )
                .build();

        assertEquals( "child", config.get( group.childSetting ) );
        assertEquals( "parent", config.get( group.parentSetting ) );
    }

    @Test
    void testValidator()
    {
        Config.Builder builder = Config.newBuilder()
                .addSettingsClass( TestSettings.class )
                .addGroupSettingClass( TestConnectionGroupSetting.class )
                .addValidator( TestConnectionGroupSetting.class )
                .set( TestConnectionGroupSetting.group( "1" ).port, 1111 )
                .set( TestConnectionGroupSetting.group( "2" ).port, 1111 );

        Exception e = assertThrows( IllegalArgumentException.class, builder::build );
        assertEquals( "Need unique ports", e.getMessage() );
    }

    @Test
    void testInvalidValidator()
    {

        Config.Builder builder = Config.newBuilder()
                .addValidator( InvalidValidator.class );

        Exception e = assertThrows( IllegalArgumentException.class, builder::build );
        assertTrue( e.getMessage().contains( "Failed to create instance of" ) );
    }

    @Test
    void testMalformedGroupSetting()
    {
        Map<String,String> settings = Map.of( "test.connection.http.1.foo.bar", "1111");

        Config.Builder builder = Config.newBuilder()
                .set( GraphDatabaseSettings.strict_config_validation, true )
                .addGroupSettingClass( TestConnectionGroupSetting.class )
                .setRaw( settings );

        assertThrows( IllegalArgumentException.class, builder::build );
    }

    @Test
    void testGetGroups()
    {
        Config config = Config.newBuilder()
                .addGroupSettingClass( TestConnectionGroupSetting.class )
                .set( TestConnectionGroupSetting.group( "default" ).port, 7474 )
                .set( TestConnectionGroupSetting.group( "1" ).port, 1111 )
                .set( TestConnectionGroupSetting.group( "1" ).hostname, "0.0.0.0" )
                .set( TestConnectionGroupSetting.group( "1" ).secure, false )
                .set( TestConnectionGroupSetting.group( "2" ).port, 2222 )
                .set( TestConnectionGroupSetting.group( "2" ).hostname, "127.0.0.1" )
                .build();

        var groups = config.getGroups( TestConnectionGroupSetting.class );
        assertEquals( Set.of( "default", "1", "2" ), groups.keySet() );
        assertEquals( 7474, config.get( groups.get( "default" ).port ) );
        assertEquals( true, config.get( groups.get( "2" ).secure ) );

    }

    @Test
    void testFromConfig()
    {
        Config fromConfig = Config.newBuilder()
                .addSettingsClass( TestSettings.class )
                .setDefault( TestSettings.boolSetting, false )
                .set( TestSettings.intSetting, 3 ).build();

        Config config1 = Config.newBuilder().fromConfig( fromConfig ).build();
        assertEquals( 3, config1.get( TestSettings.intSetting ) );
        assertEquals( "hello", config1.get( TestSettings.stringSetting ) );

        Config config2 = Config.newBuilder()
                .fromConfig( fromConfig )
                .set( TestSettings.intSetting, 5 )
                .build();

        assertEquals( 5, config2.get( TestSettings.intSetting ) );

        Config config3 = Config.newBuilder()
                .addSettingsClass( TestSettings.class )
                .fromConfig( fromConfig )
                .set( TestSettings.intSetting, 7 )
                .build();

        assertEquals( 7, config3.get( TestSettings.intSetting ) );
        assertEquals( false, config3.get( TestSettings.boolSetting ) );

    }

    @Test
    void shouldThrowIfMultipleFromConfig()
    {
        Config fromConfig = Config.newBuilder()
                .addSettingsClass( TestSettings.class )
                .setDefault( TestSettings.boolSetting, false )
                .set( TestSettings.intSetting, 3 ).build();

        assertThrows( IllegalArgumentException.class, () -> Config.newBuilder().fromConfig( fromConfig ).fromConfig( fromConfig ).build() );
    }

    @Test
    void testGroupFromConfig()
    {
        Config fromConfig = Config.newBuilder()
                .addGroupSettingClass( TestConnectionGroupSetting.class )
                .set( TestConnectionGroupSetting.group( "default" ).port, 7474 )
                .set( TestConnectionGroupSetting.group( "1" ).port, 1111 )
                .set( TestConnectionGroupSetting.group( "1" ).hostname, "0.0.0.0" )
                .set( TestConnectionGroupSetting.group( "1" ).secure, false  )
                .build();

        Config config1 = Config.newBuilder()
                .fromConfig( fromConfig )
                .build();

        var groups1 = config1.getGroups( TestConnectionGroupSetting.class );
        assertEquals( Set.of( "default", "1" ), groups1.keySet() );
        assertEquals( 7474, config1.get( groups1.get( "default" ).port ) );

        Map<String,String> settings =
                Map.of(  );
        Config config2 = Config.newBuilder()
                .fromConfig( fromConfig )
                .addGroupSettingClass( TestConnectionGroupSetting.class )
                .set( TestConnectionGroupSetting.group( "1" ).port, 3333 )
                .set( TestConnectionGroupSetting.group( "2" ).port, 2222 )
                .set( TestConnectionGroupSetting.group( "2" ).hostname, "127.0.0.1" )
                .build();

        var groups2 = config2.getGroups( TestConnectionGroupSetting.class );
        assertEquals( Set.of( "default", "1", "2" ), groups2.keySet() );
        assertEquals( 7474, config2.get( groups2.get( "default" ).port ) );
        assertEquals( 3333, config2.get( groups2.get( "1" ).port ) );
        assertEquals( true, config2.get( groups2.get( "default" ).secure ) );
        assertEquals( true, config2.get( groups2.get( "2" ).secure ) );
    }

    @Test
    void testResolveDefaultSettingDependency()
    {
        Config.Builder builder = Config.newBuilder().addSettingsClass( DependencySettings.class );

        {
            Config config = builder.build();
            assertEquals( config.get( DependencySettings.baseString ), config.get( DependencySettings.dependingString ) );
        }
        {
            String value = "default overrides dependency";
            builder.setDefault( DependencySettings.dependingString, value );
            Config config = builder.build();
            assertEquals( value, config.get( DependencySettings.dependingString ) );
        }

        {
            String value = "value overrides dependency";
            builder.set( DependencySettings.dependingString, value);
            Config config = builder.build();
            assertEquals( value, config.get( DependencySettings.dependingString ) );
        }
    }

    @Test
    void testResolvePathSettingDependency()
    {
        Config config = Config.newBuilder()
                .addSettingsClass( DependencySettings.class )
                .build();

        assertEquals( Path.of( "/base/" ).toAbsolutePath(), config.get( DependencySettings.basePath ) );
        assertEquals( Path.of( "/base/mid/" ).toAbsolutePath(), config.get( DependencySettings.midPath ) );
        assertEquals( Path.of( "/base/mid/end/file" ).toAbsolutePath(), config.get( DependencySettings.endPath ) );
        assertEquals( Path.of( "/another/path/file" ).toAbsolutePath(), config.get( DependencySettings.absolute ) );

        config.set( DependencySettings.endPath, Path.of("/path/another_file") );
        config.set( DependencySettings.absolute, Path.of("path/another_file") );
        assertEquals( Path.of( "/path/another_file" ).toAbsolutePath(), config.get( DependencySettings.endPath ) );
        assertEquals( Path.of( "/base/mid/path/another_file" ).toAbsolutePath(), config.get( DependencySettings.absolute ) );
    }

    private static final class BrokenDependencySettings implements SettingsDeclaration
    {
        static Setting<Path> broken = newBuilder( "test.base.path", PATH, Path.of( "/base/" ) )
                .setDependency( newBuilder( "test.not.present.dependency", PATH, Path.of("/broken/" ) ).immutable().build() )
                .immutable()
                .build();
    }

    @Test
    void testResolveBrokenSettingDependency()
    {
        Config.Builder builder = Config.newBuilder().addSettingsClass( BrokenDependencySettings.class );
        assertThrows( IllegalArgumentException.class, builder::build );
    }

    private static final class SingleSettingGroup extends GroupSetting
    {
        final Setting<String> singleSetting = getBuilder( STRING, null ).build();
        static SingleSettingGroup group( String name )
        {
            return new SingleSettingGroup( name );
        }
        private SingleSettingGroup( String name )
        {
            super( name );
        }

        @Override
        public String getPrefix()
        {
            return "test.single_setting";
        }
    }

    @Test
    void testSingleSettingGroup()
    {
        Map<String,String> fromSettings =
                Map.of( "test.single_setting.default", "default value",
                        "test.single_setting.foo", "foo",
                        "test.single_setting.bar", "bar" );
        Config config = Config.newBuilder()
                .addGroupSettingClass( SingleSettingGroup.class )
                .setRaw( fromSettings )
                .build();

        assertEquals( 3, config.getGroups( SingleSettingGroup.class ).size() );
        assertEquals( "default value", config.get( SingleSettingGroup.group( "default" ).singleSetting ) );
        assertEquals( "foo", config.get( SingleSettingGroup.group( "foo" ).singleSetting ) );
        assertEquals( "bar", config.get( SingleSettingGroup.group( "bar" ).singleSetting ) );
    }

    @Test
    void shouldLogIfConfigFileCouldNotBeFound()
    {
        Log log = mock( Log.class );
        File confFile = testDirectory.file( "test.conf" ); // Note: we don't create the file.

        Config config = Config.emptyBuilder().fromFileNoThrow( confFile ).build();

        config.setLogger( log );

        verify( log ).warn( "Config file [%s] does not exist.", confFile );
    }

    @Test
    void shouldLogIfConfigFileCouldNotBeRead() throws IOException
    {
        AssertableLogProvider logProvider = new AssertableLogProvider( true );
        Log log = logProvider.getLog( Config.class );
        File confFile = testDirectory.file( "test.conf" );
        assertTrue( confFile.createNewFile() );
        assumeTrue( confFile.setReadable( false ) );

        Config config = Config.emptyBuilder().fromFileNoThrow( confFile ).build();

        config.setLogger( log );

        logProvider.rawMessageMatcher().assertContains( "Unable to load config file [%s]" );
    }

    @Test
    void canReadConfigFile() throws IOException
    {
        File confFile = testDirectory.file( "test.conf" );
        Files.write( confFile.toPath(), Collections.singletonList( GraphDatabaseSettings.default_database.name() + "=foo" ) );

        assertEquals( "foo", Config.newBuilder().fromFile( confFile ).build().get( GraphDatabaseSettings.default_database ) );
        assertEquals( "foo", Config.newBuilder().fromFileNoThrow( confFile ).build().get( GraphDatabaseSettings.default_database ) );
        assertEquals( "foo", Config.newBuilder().fromFileNoThrow( confFile.toPath() ).build().get( GraphDatabaseSettings.default_database ) );
    }

    @Test
    void mustThrowIfConfigFileCouldNotBeFound()
    {
        assertThrows( IllegalArgumentException.class, () ->
        {
            File confFile = testDirectory.file( "test.conf" );

            Config.emptyBuilder().fromFile( confFile ).build();
        } );
    }

    @Test
    void mustThrowIfConfigFileCoutNotBeRead() throws IOException
    {
        File confFile = testDirectory.file( "test.conf" );
        assertTrue( confFile.createNewFile() );
        assumeTrue( confFile.setReadable( false ) );
        assertThrows( IllegalArgumentException.class, () -> Config.emptyBuilder().fromFile( confFile ).build() );
    }

    @Test
    void mustWarnIfFileContainsDuplicateSettings() throws Exception
    {
        Log log = mock( Log.class );
        File confFile = testDirectory.createFile( "test.conf" );
        Files.write( confFile.toPath(), Arrays.asList(
                ExternalSettings.initialHeapSize.name() + "=5g",
                ExternalSettings.initialHeapSize.name() + "=4g",
                ExternalSettings.initialHeapSize.name() + "=3g",
                ExternalSettings.maxHeapSize.name() + "=10g",
                ExternalSettings.maxHeapSize.name() + "=11g" ) );

        Config config = Config.newBuilder()
                .fromFile( confFile )
                .setDefault( ExternalSettings.initialHeapSize, "1g" )
                .setDefault( ExternalSettings.initialHeapSize, "2g" )
                .build();

        config.setLogger( log );

        // We should only log the warning once for each.
        verify( log ).warn( "The '%s' setting is overridden. Setting value changed from '%s' to '%s'.",
                ExternalSettings.initialHeapSize.name(), "5g", "4g" );
        verify( log ).warn( "The '%s' setting is overridden. Setting value changed from '%s' to '%s'.",
                ExternalSettings.initialHeapSize.name(), "4g", "3g" );
        verify( log ).warn( "The '%s' setting is overridden. Setting value changed from '%s' to '%s'.",
                ExternalSettings.maxHeapSize.name(), "10g", "11g" );
    }

    @Test
    void testDisableAllConnectors()
    {
        Config config = Config.newBuilder()
                .set( BoltConnector.enabled, true )
                .set( HttpConnector.enabled, true )
                .set( HttpsConnector.enabled, true ).build();

        ConfigUtils.disableAllConnectors( config );

        assertFalse( config.get( BoltConnector.enabled ) );
        assertFalse( config.get( HttpConnector.enabled ) );
        assertFalse( config.get( HttpsConnector.enabled ) );
    }

    @Test
    void testAmendIfNotSet()
    {
        Config config = Config.newBuilder().addSettingsClass( TestSettings.class ).build();
        config.setIfNotSet( TestSettings.intSetting, 77 );
        assertEquals( 77, config.get( TestSettings.intSetting ) );

        Config configWithSetting = Config.newBuilder().addSettingsClass( TestSettings.class ).set( TestSettings.intSetting, 66 ).build();
        configWithSetting.setIfNotSet( TestSettings.intSetting, 77 );
        assertEquals( 66, configWithSetting.get( TestSettings.intSetting ) );
    }

    @Test
    void testIsExplicitlySet()
    {
        Config config = Config.emptyBuilder().addSettingsClass( TestSettings.class ).build();
        assertFalse( config.isExplicitlySet( TestSettings.intSetting ) );
        config.set( TestSettings.intSetting, 77 );
        assertTrue( config.isExplicitlySet( TestSettings.intSetting ) );

        Config configWithSetting = Config.emptyBuilder().addSettingsClass( TestSettings.class ).set( TestSettings.intSetting, 66 ).build();
        assertTrue( configWithSetting.isExplicitlySet( TestSettings.intSetting ) );
        configWithSetting.set( TestSettings.intSetting, null );
        assertFalse( configWithSetting.isExplicitlySet( TestSettings.intSetting ) );
    }

    @Test
    void testDefaultDatabaseMigrator() throws IOException
    {
        File confFile = testDirectory.createFile( "test.conf" );
        Files.write( confFile.toPath(), List.of( "dbms.active_database=foo") );

        {
            Config config = Config.newBuilder()
                    .fromFile( confFile )
                    .build();
            Log log = mock( Log.class );
            config.setLogger( log );

            assertEquals( "foo", config.get( GraphDatabaseSettings.default_database ) );
            verify( log ).warn( "Use of deprecated setting %s. It is replaced by %s", "dbms.active_database", GraphDatabaseSettings.default_database.name() );
        }
        {
            Config config = Config.newBuilder()
                    .fromFile( confFile )
                    .set( GraphDatabaseSettings.default_database, "bar" )
                    .build();
            Log log = mock( Log.class );
            config.setLogger( log );

            assertEquals( "bar", config.get( GraphDatabaseSettings.default_database ) );
            verify( log ).warn( "Use of deprecated setting %s. It is replaced by %s", "dbms.active_database", GraphDatabaseSettings.default_database.name() );
        }

    }

    @Test
    void testConnectorOldFormatMigration() throws IOException
    {
        File confFile = testDirectory.createFile( "test.conf" );
        Files.write( confFile.toPath(), Arrays.asList(
                "dbms.connector.bolt.enabled=true",
                "dbms.connector.bolt.type=BOLT",
                "dbms.connector.http.enabled=true",
                "dbms.connector.https.enabled=true",
                "dbms.connector.bolt2.type=bolt",
                "dbms.connector.bolt2.listen_address=:1234" ) );

        Config config = Config.newBuilder().fromFile( confFile ).build();
        var logProvider = new AssertableLogProvider();
        config.setLogger( logProvider.getLog( Config.class ) );

        assertTrue( config.get( BoltConnector.enabled ) );
        assertTrue( config.get( HttpConnector.enabled ) );
        assertTrue( config.get( HttpsConnector.enabled ) );

        logProvider.assertAtLeastOnce( inLog( Config.class ).warn( "Use of deprecated setting %s. Type is no longer required", "dbms.connector.bolt.type" ) );
        logProvider.assertAtLeastOnce( inLog( Config.class ).warn( "Use of deprecated setting %s. No longer supports multiple connectors. Setting discarded.",
                "dbms.connector.bolt2.type" ) );
        logProvider.assertAtLeastOnce( inLog( Config.class ).warn( "Use of deprecated setting %s. No longer supports multiple connectors. Setting discarded.",
                "dbms.connector.bolt2.listen_address" ) );

    }

    @TestFactory
    Collection<DynamicTest> testConnectorAddressMigration()
    {
        Collection<DynamicTest> tests = new ArrayList<>();
        tests.add( dynamicTest( "Test bolt connector address migration",
                () -> testAddrMigration( BoltConnector.listen_address, BoltConnector.advertised_address ) ) );
        tests.add( dynamicTest( "Test http connector address migration",
                () -> testAddrMigration( HttpConnector.listen_address, HttpConnector.advertised_address ) ) );
        tests.add( dynamicTest( "Test https connector address migration",
                () -> testAddrMigration( HttpsConnector.listen_address, HttpsConnector.advertised_address ) ) );
        return tests;
    }

    private static void testAddrMigration( Setting<SocketAddress> listenAddr, Setting<SocketAddress> advertisedAddr )
    {
        Config config1 = Config.newBuilder().setRaw( Map.of( listenAddr.name(), "foo:111" ) ).build();
        Config config2 = Config.newBuilder().setRaw( Map.of( listenAddr.name(), ":222" ) ).build();
        Config config3 = Config.newBuilder().setRaw( Map.of( listenAddr.name(), ":333", advertisedAddr.name(), "bar" ) ).build();
        Config config4 = Config.newBuilder().setRaw( Map.of( listenAddr.name(), "foo:444", advertisedAddr.name(), ":555" ) ).build();
        Config config5 = Config.newBuilder().setRaw( Map.of( listenAddr.name(), "foo", advertisedAddr.name(), "bar" ) ).build();
        Config config6 = Config.newBuilder().setRaw( Map.of( listenAddr.name(), "foo:666", advertisedAddr.name(), "bar:777" ) ).build();

        var logProvider = new AssertableLogProvider();
        config1.setLogger( logProvider.getLog( Config.class ) );
        config2.setLogger( logProvider.getLog( Config.class ) );
        config3.setLogger( logProvider.getLog( Config.class ) );
        config4.setLogger( logProvider.getLog( Config.class ) );
        config5.setLogger( logProvider.getLog( Config.class ) );
        config6.setLogger( logProvider.getLog( Config.class ) );

        assertEquals( new SocketAddress( "localhost", 111 ), config1.get( advertisedAddr ) );
        assertEquals( new SocketAddress( "localhost", 222 ), config2.get( advertisedAddr ) );
        assertEquals( new SocketAddress( "bar", 333 ), config3.get( advertisedAddr ) );
        assertEquals( new SocketAddress( "localhost", 555 ), config4.get( advertisedAddr ) );
        assertEquals( new SocketAddress( "bar", advertisedAddr.defaultValue().getPort() ), config5.get( advertisedAddr ) );
        assertEquals( new SocketAddress( "bar", 777 ), config6.get( advertisedAddr ) );

        String msg = "Use of deprecated setting port propagation. port %s is migrated from %s to %s.";

        logProvider.assertAtLeastOnce( inLog( Config.class ).warn( msg, 111, listenAddr.name(), advertisedAddr.name() ) );
        logProvider.assertAtLeastOnce( inLog( Config.class ).warn( msg, 222, listenAddr.name(), advertisedAddr.name() ) );
        logProvider.assertAtLeastOnce( inLog( Config.class ).warn( msg, 333, listenAddr.name(), advertisedAddr.name() ) );

        logProvider.assertNone( inLog( Config.class ).warn( msg, 444, listenAddr.name(), advertisedAddr.name() ) );
        logProvider.assertNone( inLog( Config.class ).warn( msg, 555, listenAddr.name(), advertisedAddr.name() ) );
        logProvider.assertNone( inLog( Config.class ).warn( msg, 666, listenAddr.name(), advertisedAddr.name() ) );
    }

    @Test
    void testStrictValidation() throws IOException
    {
        File confFile = testDirectory.createFile( "test.conf" );
        Files.write( confFile.toPath(), Collections.singletonList( "some_unrecognized_garbage=true" ) );

        Config.Builder builder = Config.newBuilder().fromFile( confFile );
        builder.set( GraphDatabaseSettings.strict_config_validation, true );
        assertThrows( IllegalArgumentException.class, builder::build );

        builder.set( GraphDatabaseSettings.strict_config_validation, false );
        assertDoesNotThrow( builder::build );
    }

    @Test
    void testIncorrectType()
    {
        Map<Setting<?>, Object> cfgMap = Map.of( TestSettings.intSetting, "not an int" );
        Config.Builder builder = Config.newBuilder().addSettingsClass( TestSettings.class ).set( cfgMap );

        IllegalArgumentException exception = assertThrows( IllegalArgumentException.class, builder::build );
        assertEquals( "Error evaluating value for setting 'test.setting.integer'." +
                " Setting 'test.setting.integer' can not have value 'not an int'." +
                " Should be of type 'Integer', but is 'String'", exception.getMessage() );
    }

    @Test
    void testBoltHttpsSslPolicyMigration() throws IOException
    {

        File confFile = testDirectory.createFile( "test.conf" );
        Files.write( confFile.toPath(), List.of( "bolt.ssl_policy=foo", "https.ssl_policy=bar" ) );

        Config config = Config.newBuilder().fromFile( confFile ).build();
        var logProvider = new AssertableLogProvider();
        config.setLogger( logProvider.getLog( Config.class ) );

        assertEquals( "foo", config.get( BoltConnector.ssl_policy ) );
        assertEquals( "bar", config.get( HttpsConnector.ssl_policy ) );

        String msg = "Use of deprecated setting %s. It is replaced by %s";
        logProvider.assertAtLeastOnce( inLog( Config.class ).warn( msg, "bolt.ssl_policy", BoltConnector.ssl_policy.name() ) );
        logProvider.assertAtLeastOnce( inLog( Config.class ).warn( msg, "https.ssl_policy", HttpsConnector.ssl_policy.name() ) );

    }

    @Test
    void testDoesNotLogChangedJvmArgs() throws IOException
    {
        File confFile = testDirectory.createFile( "test.conf" );
        Files.write( confFile.toPath(), List.of( "dbms.jvm.additional=-XX:+UseG1GC", "dbms.jvm.additional=-XX:+AlwaysPreTouch",
                "dbms.jvm.additional=-XX:+UnlockExperimentalVMOptions", "dbms.jvm.additional=-XX:+TrustFinalNonStaticFields" ) );

        Config config = Config.newBuilder().fromFile( confFile ).build();
        var logProvider = new AssertableLogProvider();
        config.setLogger( logProvider.getLog( Config.class ) );

        logProvider.assertNoLoggingOccurred();
    }

    private static final class TestSettings implements SettingsDeclaration
    {
        static Setting<String> stringSetting = newBuilder( "test.setting.string", STRING, "hello" ).build();
        static Setting<Integer> intSetting = newBuilder( "test.setting.integer", INT, 1 ).dynamic().build();
        static Setting<List<Integer>> intListSetting = newBuilder( "test.setting.integerlist", listOf( INT ), List.of( 1 ) ).build();
        static Setting<Boolean> boolSetting = newBuilder( "test.setting.bool", BOOL, null ).immutable().build();
    }

    public static class TestConnectionGroupSetting extends GroupSetting implements GroupSettingValidator
    {

        public static TestConnectionGroupSetting group( String name )
        {
            return new TestConnectionGroupSetting( name );
        }
        @Override
        public String getPrefix()
        {
            return "test.connection.http";
        }

        public final Setting<Integer> port = getBuilder( "port", INT, 1 ).build();

        public final Setting<String> hostname = getBuilder( "hostname" , STRING, "0.0.0.0" ).build();
        public final Setting<Boolean> secure = getBuilder( "secure", BOOL, true ).build();
        TestConnectionGroupSetting( String id )
        {
            super( id );
        }

        @Override
        public void validate( Map<Setting<?>,Object> values, Config config )
        {
            Set<Integer> ports = new HashSet<>();
            values.forEach( ( S, V ) ->
            {
                if ( ((SettingImpl<Object>) S).suffix().equals( "port" ) )
                {
                    if ( !ports.add( (Integer) V ) )
                    {
                        throw new IllegalArgumentException( "Need unique ports" );
                    }
                }
            } );
        }

        @Override
        public String getDescription()
        {
            return "With unique ports";
        }

    }

    static class InvalidValidator implements GroupSettingValidator
    {
        InvalidValidator( Object invalidConstructor )
        {
        }

        @Override
        public String getPrefix()
        {
            return "test.validator";
        }

        @Override
        public String getDescription()
        {
            return null;
        }

        @Override
        public void validate( Map<Setting<?>,Object> values, Config config )
        {

        }
    }

    abstract static class ParentGroup extends GroupSetting
    {

        final Setting<String> parentSetting = getBuilder( "parent" , STRING, "parent" ).build();

        ParentGroup( String name )
        {
            super( name );
        }

    }
    static class ChildGroup extends ParentGroup
    {
        final Setting<String> childSetting = getBuilder( "child" , STRING, null ).build();
        private ChildGroup( String name )
        {
            super( name );
        }
        @Override
        public String getPrefix()
        {
            return "test.inheritance";
        }

    }

    private static final class DependencySettings implements SettingsDeclaration
    {
        static Setting<Path> basePath = newBuilder( "test.base.path", PATH, Path.of( "/base/" ).toAbsolutePath() ).immutable().build();
        static Setting<Path> midPath = newBuilder( "test.mid.path", PATH, Path.of( "mid/" ) ).setDependency( basePath ).immutable().build();
        static Setting<Path> endPath = newBuilder( "test.end.path", PATH, Path.of( "end/file" ) ).setDependency( midPath ).build();
        static Setting<Path> absolute =
                newBuilder( "test.absolute.path", PATH, Path.of( "/another/path/file" ).toAbsolutePath() ).setDependency( midPath ).build();

        private static SettingValueParser<String> DefaultParser = new SettingValueParser<>()
        {
            @Override
            public String parse( String value )
            {
                return value;
            }

            @Override
            public String getDescription()
            {
                return "";
            }

            @Override
            public Class<String> getType()
            {
                return String.class;
            }
        };

        static Setting<String> baseString = newBuilder( "test.default.dependency.base", DefaultParser, "base" ).immutable().build();

        static Setting<String> dependingString = newBuilder( "test.default.dependency.dep", DefaultParser, null ).setDependency( baseString ).build();
    }

}

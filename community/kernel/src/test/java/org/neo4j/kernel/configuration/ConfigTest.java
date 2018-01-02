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
package org.neo4j.kernel.configuration;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InOrder;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;

import org.neo4j.configuration.DocumentedDefaultValue;
import org.neo4j.configuration.Dynamic;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.configuration.ReplacedBy;
import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.logging.Log;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.setting;

public class ConfigTest
{

    private static final String ORIGIN = "test";

    public static class MyMigratingSettings implements LoadableConfig
    {
        @SuppressWarnings( "unused" ) // accessed by reflection
        @Migrator
        public static ConfigurationMigrator migrator = new BaseConfigurationMigrator()
        {
            {
                add( new SpecificPropertyMigration( "old", "Old has been replaced by newer!" )
                {
                    @Override
                    public void setValueWithOldSetting( String value, Map<String,String> rawConfiguration )
                    {
                        rawConfiguration.put( newer.name(), value );
                    }
                } );
            }
        };

        public static Setting<String> newer = setting( "newer", STRING, "" );
    }

    public static class MySettingsWithDefaults implements LoadableConfig
    {
        public static final Setting<String> hello = setting( "hello", STRING, "Hello, World!" );

        public static final Setting<Boolean> boolSetting = setting( "bool_setting", BOOLEAN, Settings.TRUE );

        @Internal
        @DocumentedDefaultValue( "<documented default value>" )
        public static final Setting<Boolean> secretSetting = setting( "secret_setting", BOOLEAN, Settings.TRUE );

        @Deprecated
        @ReplacedBy( "hello" )
        public static final Setting<String> oldHello = setting( "old_hello", STRING, "Hello, Bob" );

        @Deprecated
        public static final Setting<String> oldSetting = setting( "some_setting", STRING, "Has no replacement" );
    }

    private static class HelloHasToBeNeo4jConfigurationValidator implements ConfigurationValidator
    {
        @Override
        public Map<String,String> validate( @Nonnull Config config, @Nonnull Log log ) throws InvalidSettingException
        {
            if ( !config.get( MySettingsWithDefaults.hello ).equals( "neo4j" ) )
            {
                throw new InvalidSettingException( "Setting hello has to set to neo4j" );
            }

            return Collections.emptyMap();
        }
    }

    private static MyMigratingSettings myMigratingSettings = new MyMigratingSettings();
    private static MySettingsWithDefaults mySettingsWithDefaults = new MySettingsWithDefaults();

    private static Config Config()
    {
        return Config( Collections.emptyMap() );
    }

    private static Config Config( Map<String,String> params )
    {
        return Config.fromSettings( params )
                     .withConfigClasses( Arrays.asList( mySettingsWithDefaults, myMigratingSettings ) ).build();
    }

    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    @Rule
    public ExpectedException expect = ExpectedException.none();

    @Test
    public void shouldApplyDefaults()
    {
        Config config = Config();

        assertThat( config.get( MySettingsWithDefaults.hello ), is( "Hello, World!" ) );
    }

    @Test
    public void shouldApplyMigrations()
    {
        // When
        Config config = Config( stringMap( "old", "hello!" ) );

        // Then
        assertThat( config.get( MyMigratingSettings.newer ), is( "hello!" ) );
    }

    @Test( expected = InvalidSettingException.class )
    public void shouldNotAllowSettingInvalidValues()
    {
        Config( stringMap( MySettingsWithDefaults.boolSetting.name(), "asd" ) );
        fail( "Expected validation to fail." );
    }

    @Test
    public void shouldBeAbleToAugmentConfig() throws Exception
    {
        // Given
        Config config = Config();

        // When
        config.augment( MySettingsWithDefaults.boolSetting, Settings.FALSE );
        config.augment( MySettingsWithDefaults.hello, "Bye" );

        // Then
        assertThat( config.get( MySettingsWithDefaults.boolSetting ), equalTo( false ) );
        assertThat( config.get( MySettingsWithDefaults.hello ), equalTo( "Bye" ) );
    }

    @Test
    public void augmentAnotherConfig() throws Exception
    {
        Config config = Config();
        config.augment( MySettingsWithDefaults.hello, "Hi" );

        Config anotherConfig = Config();
        anotherConfig.augment( stringMap( MySettingsWithDefaults.boolSetting.name(),
                Settings.FALSE, MySettingsWithDefaults.hello.name(), "Bye" ) );

        config.augment( anotherConfig );

        assertThat( config.get( MySettingsWithDefaults.boolSetting ), equalTo( false ) );
        assertThat( config.get( MySettingsWithDefaults.hello ), equalTo( "Bye" ) );
    }

    @Test
    public void shouldWarnAndDiscardUnknownOptionsInReservedNamespaceAndPassOnBufferedLogInWithMethods()
            throws Exception
    {
        // Given
        Log log = mock( Log.class );
        File confFile = testDirectory.file( "test.conf" );
        assertTrue( confFile.createNewFile() );

        Config config = Config.fromFile( confFile )
                              .withSetting( GraphDatabaseSettings.strict_config_validation, "false" )
                              .withSetting( "ha.jibberish", "baah" )
                              .withSetting( "dbms.jibberish", "booh" ).build();

        // When
        config.setLogger( log );
        config.augment( "causal_clustering.jibberish", "baah" );

        // Then
        verify( log ).warn( "Unknown config option: %s", "dbms.jibberish" );
        verify( log ).warn( "Unknown config option: %s", "ha.jibberish" );
        verifyNoMoreInteractions( log );
    }

    @Test
    public void shouldLogDeprecationWarnings()
            throws Exception
    {
        // Given
        Log log = mock( Log.class );
        File confFile = testDirectory.file( "test.conf" );
        assertTrue( confFile.createNewFile() );

        Config config = Config.fromFile( confFile )
                              .withSetting( MySettingsWithDefaults.oldHello, "baah" )
                              .withSetting( MySettingsWithDefaults.oldSetting, "booh" )
                              .withConfigClasses( Arrays.asList( mySettingsWithDefaults, myMigratingSettings,
                                      new GraphDatabaseSettings() ) )
                              .build();

        // When
        config.setLogger( log );

        // Then
        verify( log ).warn( "%s is deprecated. Replaced by %s", MySettingsWithDefaults.oldHello.name(),
                MySettingsWithDefaults.hello.name() );
        verify( log ).warn( "%s is deprecated.", MySettingsWithDefaults.oldSetting.name() );
        verifyNoMoreInteractions( log );
    }

    @Test
    public void shouldSetInternalParameter()
            throws Exception
    {
        // Given
        Config config = Config.builder()
                              .withSetting( MySettingsWithDefaults.secretSetting, "false" )
                              .withSetting( MySettingsWithDefaults.hello, "ABC" )
                              .withConfigClasses( Arrays.asList( mySettingsWithDefaults, myMigratingSettings ) )
                              .build();

        // Then
        assertTrue( config.getConfigValues().get( MySettingsWithDefaults.secretSetting.name() ).internal() );
        assertFalse( config.getConfigValues().get( MySettingsWithDefaults.hello.name() ).internal() );
    }

    @Test
    public void shouldSetDocumentedDefaultValue()
            throws Exception
    {
        // Given
        Config config = Config.builder()
                              .withSetting( MySettingsWithDefaults.secretSetting, "false" )
                              .withSetting( MySettingsWithDefaults.hello, "ABC" )
                              .withConfigClasses( Arrays.asList( new MySettingsWithDefaults(), myMigratingSettings ) )
                              .build();

        // Then
        assertEquals( Optional.of( "<documented default value>" ),
                config.getConfigValues().get( MySettingsWithDefaults.secretSetting.name() )
                      .documentedDefaultValue() );
        assertEquals( Optional.empty(),
                config.getConfigValues().get( MySettingsWithDefaults.hello.name() ).documentedDefaultValue() );
    }

    @Test
    public void validatorsShouldBeCalledWhenBuilding() throws Exception
    {
        // Should not throw
        Config.builder()
              .withSetting( MySettingsWithDefaults.hello, "neo4j" )
              .withValidator( new HelloHasToBeNeo4jConfigurationValidator() )
              .withConfigClasses( Arrays.asList( mySettingsWithDefaults, myMigratingSettings ) ).build();

        expect.expect( InvalidSettingException.class );
        expect.expectMessage( "Setting hello has to set to neo4j" );

        // Should throw
        Config.builder()
              .withSetting( MySettingsWithDefaults.hello, "not-neo4j" )
              .withValidator( new HelloHasToBeNeo4jConfigurationValidator() )
              .withConfigClasses( Arrays.asList( mySettingsWithDefaults, myMigratingSettings ) ).build();
    }

    @Group( "a.b.c" )
    private static class GroupedSetting
    {
    }

    @Test
    public void identifiersFromGroup() throws Exception
    {
        // Given
        File confFile = testDirectory.file( "test.conf" );
        assertTrue( confFile.createNewFile() );

        Config config = Config.fromFile( confFile )
                              .withSetting( GraphDatabaseSettings.strict_config_validation, "false" )
                              .withSetting( "a.b.c.first.jibberish", "baah" )
                              .withSetting( "a.b.c.second.jibberish", "baah" )
                              .withSetting( "a.b.c.third.jibberish", "baah" )
                              .withSetting( "a.b.c.forth.jibberish", "baah" ).build();

        Set<String> identifiers = config.identifiersFromGroup( GroupedSetting.class );
        Set<String> expectedIdentifiers = new HashSet<>( Arrays.asList( "first", "second", "third", "forth" ) );

        assertEquals( expectedIdentifiers, identifiers );
    }

    @Test
    public void isConfigured() throws Exception
    {
        Config config = Config();
        assertFalse( config.isConfigured( MySettingsWithDefaults.hello ) );
        config.augment( MySettingsWithDefaults.hello, "Hi" );
        assertTrue( config.isConfigured( MySettingsWithDefaults.hello ) );
    }

    @Test
    public void isConfiguredShouldNotReturnTrueEvenThoughDefaultValueExists() throws Exception
    {
        Config config = Config();
        assertFalse( config.isConfigured( MySettingsWithDefaults.hello ) );
        assertEquals( "Hello, World!", config.get( MySettingsWithDefaults.hello ) );
    }

    @Test
    public void withConnectorsDisabled() throws Exception
    {
        Connector httpConnector = new HttpConnector();
        Connector boltConnector = new BoltConnector();
        Config config = Config.builder()
                              .withSetting( httpConnector.enabled, "true" )
                              .withSetting( httpConnector.type, Connector.ConnectorType.HTTP.name() )
                              .withSetting( boltConnector.enabled, "true" )
                              .withSetting( boltConnector.type, Connector.ConnectorType.BOLT.name() )
                              .withConnectorsDisabled().build();
        assertFalse( config.get( httpConnector.enabled ) );
        assertFalse( config.get( boltConnector.enabled ) );
    }

    @Test
    public void augmentDefaults() throws Exception
    {
        Config config = Config();
        assertEquals( "Hello, World!", config.get( MySettingsWithDefaults.hello ) );
        config.augmentDefaults( MySettingsWithDefaults.hello, "new default" );
        assertEquals( "new default", config.get( MySettingsWithDefaults.hello ) );
    }

    public static class MyDynamicSettings implements LoadableConfig
    {
        @Dynamic
        public static final Setting<Boolean> boolSetting = setting( "bool_setting", BOOLEAN, Settings.TRUE );
    }

    @Test
    public void updateDynamicShouldLogChanges() throws Exception
    {
        String settingName = MyDynamicSettings.boolSetting.name();
        String changedMessage = "Setting changed: '%s' changed from '%s' to '%s' via '%s'";
        Config config = Config.builder().withConfigClasses( singletonList( new MyDynamicSettings() ) ).build();

        Log log = mock( Log.class );
        config.setLogger( log );

        config.updateDynamicSetting( settingName, "false", ORIGIN );
        config.updateDynamicSetting( settingName, "true", ORIGIN );
        config.updateDynamicSetting( settingName, "", ORIGIN );

        InOrder order = inOrder( log );
        order.verify( log ).info( changedMessage, settingName, "default (true)", "false", "test" );
        order.verify( log ).info( changedMessage, settingName, "false", "true", "test" );
        order.verify( log ).info( changedMessage, settingName, "true", "default (true)", "test" );
        verifyNoMoreInteractions( log );
    }

    @Test
    public void updateDynamicShouldThrowIfSettingIsNotDynamic() throws Exception
    {
        Config config = Config.builder().withConfigClasses( singletonList( mySettingsWithDefaults ) ).build();
        expect.expect( IllegalArgumentException.class );
        config.updateDynamicSetting( MySettingsWithDefaults.hello.name(), "hello", ORIGIN );
    }

    @Test
    public void updateDynamicShouldInformRegisteredListeners() throws Exception
    {
        Config config = Config.builder().withConfigClasses( singletonList( new MyDynamicSettings() ) ).build();
        AtomicInteger counter = new AtomicInteger( 0 );
        config.registerDynamicUpdateListener( MyDynamicSettings.boolSetting, ( previous, update ) ->
        {
            counter.getAndIncrement();
            assertTrue( previous );
            assertFalse( update );
        } );
        config.updateDynamicSetting( MyDynamicSettings.boolSetting.name(), "false", ORIGIN );
        assertThat( counter.get(), is( 1 ) );
    }

    @Test
    public void updateDynamicShouldNotAllowInvalidSettings() throws Exception
    {
        Config config = Config.builder().withConfigClasses( singletonList( new MyDynamicSettings() ) ).build();
        expect.expect( InvalidSettingException.class );
        config.updateDynamicSetting( MyDynamicSettings.boolSetting.name(), "this is not a boolean", ORIGIN );
    }

    @Test
    public void registeringUpdateListenerOnNonDynamicSettingMustThrow() throws Exception
    {
        Config config = Config.builder().withConfigClasses( singletonList( mySettingsWithDefaults ) ).build();
        expect.expect( IllegalArgumentException.class );
        config.registerDynamicUpdateListener( MySettingsWithDefaults.hello, ( a, b ) -> fail( "never called" ) );
    }

    @Test
    public void updateDynamicShouldLogExceptionsFromUpdateListeners() throws Exception
    {
        Config config = Config.builder().withConfigClasses( singletonList( new MyDynamicSettings() ) ).build();
        IllegalStateException exception = new IllegalStateException( "Boo" );
        config.registerDynamicUpdateListener( MyDynamicSettings.boolSetting, ( a, b ) ->
        {
            throw exception;
        } );
        Log log = mock( Log.class );
        config.setLogger( log );
        String settingName = MyDynamicSettings.boolSetting.name();

        config.updateDynamicSetting( settingName, "", ORIGIN );

        verify( log ).error( "Failure when notifying listeners after dynamic setting change; " +
                             "new setting might not have taken effect: Boo", exception );
    }
}

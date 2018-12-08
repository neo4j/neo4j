/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.configuration;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import javax.annotation.Nonnull;

import org.neo4j.configuration.DocumentedDefaultValue;
import org.neo4j.configuration.Dynamic;
import org.neo4j.configuration.ExternalSettings;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.configuration.ReplacedBy;
import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.logging.Log;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.setting;

@ExtendWith( TestDirectoryExtension.class )
class ConfigTest
{
    @Inject
    private TestDirectory testDirectory;

    private static final String ORIGIN = "test";
    private static MyMigratingSettings myMigratingSettings = new MyMigratingSettings();
    private static MySettingsWithDefaults mySettingsWithDefaults = new MySettingsWithDefaults();

    @Test
    void shouldApplyDefaults()
    {
        Config config = Config();

        assertThat( config.get( MySettingsWithDefaults.hello ), is( "Hello, World!" ) );
    }

    @Test
    void shouldApplyMigrations()
    {
        // When
        Config config = Config( stringMap( "old", "hello!" ) );

        // Then
        assertThat( config.get( MyMigratingSettings.newer ), is( "hello!" ) );
    }

    @Test
    void shouldNotAllowSettingInvalidValues()
    {
        assertThrows( InvalidSettingException.class, () -> Config( stringMap( MySettingsWithDefaults.boolSetting.name(), "asd" ) ) );
    }

    @Test
    void shouldBeAbleToAugmentConfig()
    {
        // Given
        Config config = Config();

        assertTrue( config.get( MySettingsWithDefaults.boolSetting ) );
        assertEquals( "Hello, World!",  config.get( MySettingsWithDefaults.hello ) );

        // When
        config.augment( MySettingsWithDefaults.boolSetting, Settings.FALSE );
        config.augment( MySettingsWithDefaults.hello, "Bye" );

        // Then
        assertThat( config.get( MySettingsWithDefaults.boolSetting ), equalTo( false ) );
        assertThat( config.get( MySettingsWithDefaults.hello ), equalTo( "Bye" ) );
    }

    @Test
    void augmentAnotherConfig()
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
    void shouldWarnAndDiscardUnknownOptionsInReservedNamespaceAndPassOnBufferedLogInWithMethods() throws Exception
    {
        // Given
        Log log = mock( Log.class );
        File confFile = testDirectory.file( "test.conf" );
        assertTrue( confFile.createNewFile() );

        Config config = Config.fromFile( confFile )
                              .withSetting( GraphDatabaseSettings.strict_config_validation, "false" )
                              .withSetting( "dbms.jibberish", "booh" ).build();

        // When
        config.setLogger( log );
        config.augment( "causal_clustering.jibberish", "baah" );

        // Then
        verify( log ).warn( "Unknown config option: %s", "dbms.jibberish" );
        verifyNoMoreInteractions( log );
    }

    @Test
    void shouldLogDeprecationWarnings() throws Exception
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
    void shouldLogIfConfigFileCouldNotBeFound()
    {
        Log log = mock( Log.class );
        File confFile = testDirectory.file( "test.conf" ); // Note: we don't create the file.

        Config config = Config.fromFile( confFile ).withNoThrowOnFileLoadFailure().build();

        config.setLogger( log );

        verify( log ).warn( "Config file [%s] does not exist.", confFile );
    }

    @Test
    void shouldLogIfConfigFileCouldNotBeRead() throws IOException
    {
        Log log = mock( Log.class );
        File confFile = testDirectory.file( "test.conf" );
        assertTrue( confFile.createNewFile() );
        assumeTrue( confFile.setReadable( false ) );

        Config config = Config.fromFile( confFile ).withNoThrowOnFileLoadFailure().build();

        config.setLogger( log );

        verify( log ).error( "Unable to load config file [%s]: %s", confFile, confFile + " (Permission denied)" );
    }

    @Test
    void mustThrowIfConfigFileCouldNotBeFound()
    {
        assertThrows( ConfigLoadIOException.class, () ->
        {
            File confFile = testDirectory.file( "test.conf" );

            Config.fromFile( confFile ).build();
        } );
    }

    @Test
    void mustThrowIfConfigFileCoutNotBeRead() throws IOException
    {
        File confFile = testDirectory.file( "test.conf" );
        assertTrue( confFile.createNewFile() );
        assumeTrue( confFile.setReadable( false ) );
        assertThrows( ConfigLoadIOException.class, () ->
        {
            Config.fromFile( confFile ).build();
        } );
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
                ExternalSettings.maxHeapSize.name() + "=10g" ) );

        Config config = Config.fromFile( confFile ).build();
        config.setLogger( log );

        // We should only log the warning once for each.
        verify( log ).warn( "The '%s' setting is specified more than once. Settings only be specified once, to avoid ambiguity. " +
                        "The setting value that will be used is '%s'.",
                ExternalSettings.initialHeapSize.name(), "5g" );
        verify( log ).warn( "The '%s' setting is specified more than once. Settings only be specified once, to avoid ambiguity. " +
                        "The setting value that will be used is '%s'.",
                ExternalSettings.maxHeapSize.name(), "10g" );
    }

    @Test
    void mustNotWarnAboutDuplicateJvmAdditionalSettings() throws Exception
    {
        Log log = mock( Log.class );
        File confFile = testDirectory.createFile( "test.conf" );
        Files.write( confFile.toPath(), Arrays.asList(
                ExternalSettings.additionalJvm.name() + "=-Dsysprop=val",
                ExternalSettings.additionalJvm.name() + "=-XX:+UseG1GC",
                ExternalSettings.additionalJvm.name() + "=-XX:+AlwaysPreTouch" ) );

        Config config = Config.fromFile( confFile ).build();
        config.setLogger( log );

        // The ExternalSettings.additionalJvm setting is allowed to be specified more than once.
        verifyNoMoreInteractions( log );
    }

    @Test
    void shouldSetInternalParameter()
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
    void shouldSetDocumentedDefaultValue()
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
    void validatorsShouldBeCalledWhenBuilding()
    {
        // Should not throw
        Config.builder()
              .withSetting( MySettingsWithDefaults.hello, "neo4j" )
              .withValidator( new HelloHasToBeNeo4jConfigurationValidator() )
              .withConfigClasses( Arrays.asList( mySettingsWithDefaults, myMigratingSettings ) ).build();

        InvalidSettingException exception = assertThrows( InvalidSettingException.class,
                        () -> Config.builder().withSetting( MySettingsWithDefaults.hello, "not-neo4j" )
                                .withValidator( new HelloHasToBeNeo4jConfigurationValidator() )
                                .withConfigClasses( Arrays.asList( mySettingsWithDefaults, myMigratingSettings ) ).build() );
        assertThat( exception.getMessage(), Matchers.containsString( "Setting hello has to set to neo4j" ) );
    }

    @Group( "a.b.c" )
    private static class GroupedSetting
    {
    }

    @Test
    void identifiersFromGroup() throws Exception
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
    void isConfigured()
    {
        Config config = Config();
        assertFalse( config.isConfigured( MySettingsWithDefaults.hello ) );
        config.augment( MySettingsWithDefaults.hello, "Hi" );
        assertTrue( config.isConfigured( MySettingsWithDefaults.hello ) );
    }

    @Test
    void isConfiguredShouldNotReturnTrueEvenThoughDefaultValueExists()
    {
        Config config = Config();
        assertFalse( config.isConfigured( MySettingsWithDefaults.hello ) );
        assertEquals( "Hello, World!", config.get( MySettingsWithDefaults.hello ) );
    }

    @Test
    void withConnectorsDisabled()
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
    void augmentDefaults()
    {
        Config config = Config();
        assertEquals( "Hello, World!", config.get( MySettingsWithDefaults.hello ) );
        config.augmentDefaults( MySettingsWithDefaults.hello, "new default" );
        assertEquals( "new default", config.get( MySettingsWithDefaults.hello ) );
    }

    @Test
    void updateDynamicShouldLogChanges()
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
    void doNotParsePropertyOnEachLookup()
    {
        ParseCounterSettings counterSettings = new ParseCounterSettings();
        Config config = Config.builder().withConfigClasses( singletonList( counterSettings ) ).build();
        // we parse property first time during validation and throw that value away
        assertEquals( 1, ParseCounterSettings.countingIntegerConverter.getCounterValue() );

        assertEquals( 1, (int) config.get( ParseCounterSettings.integerSetting ) );
        assertEquals( 2, ParseCounterSettings.countingIntegerConverter.getCounterValue() );

        assertEquals( 1, (int) config.get( ParseCounterSettings.integerSetting ) );
        assertEquals( 1, (int) config.get( ParseCounterSettings.integerSetting ) );
        assertEquals( 1, (int) config.get( ParseCounterSettings.integerSetting ) );
        assertEquals( 2, ParseCounterSettings.countingIntegerConverter.getCounterValue() );
    }

    @Test
    void updateDynamicConfig()
    {
        Config config = Config.builder().withConfigClasses( singletonList( new MyDynamicSettings() ) ).build();
        assertTrue( config.get( MyDynamicSettings.boolSetting ) );
        config.updateDynamicSetting( MyDynamicSettings.boolSetting.name(), Settings.FALSE, "test" );
        assertFalse( config.get( MyDynamicSettings.boolSetting ) );
    }

    @Test
    void updateDynamicShouldThrowIfSettingIsNotDynamic()
    {
        Config config = Config.builder().withConfigClasses( singletonList( mySettingsWithDefaults ) ).build();
        assertThrows( IllegalArgumentException.class, () -> config.updateDynamicSetting( MySettingsWithDefaults.hello.name(), "hello", ORIGIN ) );
    }

    @Test
    void updateDynamicShouldInformRegisteredListeners()
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
    void updateDynamicShouldNotAllowInvalidSettings()
    {
        Config config = Config.builder().withConfigClasses( singletonList( new MyDynamicSettings() ) ).build();
        assertThrows( InvalidSettingException.class,
                () -> config.updateDynamicSetting( MyDynamicSettings.boolSetting.name(), "this is not a boolean", ORIGIN ) );
    }

    @Test
    void registeringUpdateListenerOnNonDynamicSettingMustThrow()
    {
        Config config = Config.builder().withConfigClasses( singletonList( mySettingsWithDefaults ) ).build();
        assertThrows( IllegalArgumentException.class,
                () -> config.registerDynamicUpdateListener( MySettingsWithDefaults.hello, ( a, b ) -> fail( "never called" ) ) );
    }

    @Test
    void updateDynamicShouldLogExceptionsFromUpdateListeners()
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

    @Test
    void removeRegisteredSettingChangeListener()
    {
        Config config = Config.builder().withConfigClasses( singletonList( new MyDynamicSettings() ) ).build();
        AtomicInteger updateCounter = new AtomicInteger( 0 );
        SettingChangeListener<Boolean> listener = ( previous, update ) -> updateCounter.getAndIncrement();

        config.registerDynamicUpdateListener( MyDynamicSettings.boolSetting, listener );
        config.updateDynamicSetting( MyDynamicSettings.boolSetting.name(), "false", ORIGIN );
        assertThat( updateCounter.get(), is( 1 ) );

        config.unregisterDynamicUpdateListener( MyDynamicSettings.boolSetting, listener );

        config.updateDynamicSetting( MyDynamicSettings.boolSetting.name(), "true", ORIGIN );
        assertThat( updateCounter.get(), is( 1 ) );
    }

    private static Config Config()
    {
        return Config( Collections.emptyMap() );
    }

    private static Config Config( Map<String,String> params )
    {
        return Config.fromSettings( params )
                .withConfigClasses( Arrays.asList( mySettingsWithDefaults, myMigratingSettings ) ).build();
    }

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

    public static class MyDynamicSettings implements LoadableConfig
    {
        @Dynamic
        public static final Setting<Boolean> boolSetting = setting( "bool_setting", BOOLEAN, Settings.TRUE );
    }

    public static class ParseCounterSettings implements LoadableConfig
    {
        static final CountingIntegerConverter countingIntegerConverter = new CountingIntegerConverter();
        @Dynamic
        public static final Setting<Integer> integerSetting = setting( "integer_setting", countingIntegerConverter, "1" );

        private static class CountingIntegerConverter implements Function<String,Integer>
        {
            private final AtomicInteger counter = new AtomicInteger();

            @Override
            public Integer apply( String rawValue )
            {
                counter.incrementAndGet();
                return Integer.valueOf( rawValue );
            }

            int getCounterValue()
            {
                return counter.get();
            }
        }
    }

}

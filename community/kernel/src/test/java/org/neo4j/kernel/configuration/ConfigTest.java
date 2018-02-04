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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.rules.ExpectedException;
import org.mockito.InOrder;

import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import javax.annotation.Resource;

import org.neo4j.configuration.DocumentedDefaultValue;
import org.neo4j.configuration.Dynamic;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.configuration.ReplacedBy;
import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.logging.Log;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.rules.ExpectedException.none;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.strict_config_validation;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.configuration.Config.builder;
import static org.neo4j.kernel.configuration.Config.fromFile;
import static org.neo4j.kernel.configuration.Config.fromSettings;
import static org.neo4j.kernel.configuration.ConfigTest.MyMigratingSettings.newer;
import static org.neo4j.kernel.configuration.ConfigTest.MySettingsWithDefaults.boolSetting;
import static org.neo4j.kernel.configuration.ConfigTest.MySettingsWithDefaults.hello;
import static org.neo4j.kernel.configuration.ConfigTest.MySettingsWithDefaults.oldHello;
import static org.neo4j.kernel.configuration.ConfigTest.MySettingsWithDefaults.oldSetting;
import static org.neo4j.kernel.configuration.ConfigTest.MySettingsWithDefaults.secretSetting;
import static org.neo4j.kernel.configuration.Connector.ConnectorType.BOLT;
import static org.neo4j.kernel.configuration.Connector.ConnectorType.HTTP;
import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.TRUE;
import static org.neo4j.kernel.configuration.Settings.setting;

@EnableRuleMigrationSupport
@ExtendWith( TestDirectoryExtension.class )
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

        public static final Setting<Boolean> boolSetting = setting( "bool_setting", BOOLEAN, TRUE );

        @Internal
        @DocumentedDefaultValue( "<documented default value>" )
        public static final Setting<Boolean> secretSetting = setting( "secret_setting", BOOLEAN, TRUE );

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
            if ( !config.get( hello ).equals( "neo4j" ) )
            {
                throw new InvalidSettingException( "Setting hello has to set to neo4j" );
            }

            return emptyMap();
        }
    }

    private static MyMigratingSettings myMigratingSettings = new MyMigratingSettings();
    private static MySettingsWithDefaults mySettingsWithDefaults = new MySettingsWithDefaults();

    private static Config Config()
    {
        return Config( emptyMap() );
    }

    private static Config Config( Map<String,String> params )
    {
        return fromSettings( params ).withConfigClasses( asList( mySettingsWithDefaults, myMigratingSettings ) ).build();
    }

    @Resource
    public TestDirectory testDirectory;

    @Rule
    public ExpectedException expect = none();

    @Test
    public void shouldApplyDefaults()
    {
        Config config = Config();

        assertThat( config.get( hello ), is( "Hello, World!" ) );
    }

    @Test
    public void shouldApplyMigrations()
    {
        // When
        Config config = Config( stringMap( "old", "hello!" ) );

        // Then
        assertThat( config.get( newer ), is( "hello!" ) );
    }

    @Test
    public void shouldNotAllowSettingInvalidValues()
    {
        assertThrows( InvalidSettingException.class, () -> {
            Config( stringMap( boolSetting.name(), "asd" ) );
            fail( "Expected validation to fail." );
        } );
    }

    @Test
    public void shouldBeAbleToAugmentConfig()
    {
        // Given
        Config config = Config();

        // When
        config.augment( boolSetting, FALSE );
        config.augment( hello, "Bye" );

        // Then
        assertThat( config.get( boolSetting ), equalTo( false ) );
        assertThat( config.get( hello ), equalTo( "Bye" ) );
    }

    @Test
    public void augmentAnotherConfig()
    {
        Config config = Config();
        config.augment( hello, "Hi" );

        Config anotherConfig = Config();
        anotherConfig.augment( stringMap( boolSetting.name(), FALSE, hello.name(), "Bye" ) );

        config.augment( anotherConfig );

        assertThat( config.get( boolSetting ), equalTo( false ) );
        assertThat( config.get( hello ), equalTo( "Bye" ) );
    }

    @Test
    public void shouldWarnAndDiscardUnknownOptionsInReservedNamespaceAndPassOnBufferedLogInWithMethods()
            throws Exception
    {
        // Given
        Log log = mock( Log.class );
        File confFile = testDirectory.file( "test.conf" );
        assertTrue( confFile.createNewFile() );

        Config config = fromFile( confFile ).withSetting( strict_config_validation, "false" )
                .withSetting( "ha.jibberish", "baah" ).withSetting( "dbms.jibberish", "booh" ).build();

        // When
        config.setLogger( log );
        config.augment( "causal_clustering.jibberish", "baah" );

        // Then
        verify( log ).warn( "Unknown config option: %s", "dbms.jibberish" );
        verify( log ).warn( "Unknown config option: %s", "ha.jibberish" );
        verifyNoMoreInteractions( log );
    }

    @Test
    public void shouldLogDeprecationWarnings() throws Exception
    {
        // Given
        Log log = mock( Log.class );
        File confFile = testDirectory.file( "test.conf" );
        assertTrue( confFile.createNewFile() );

        Config config = fromFile( confFile ).withSetting( oldHello, "baah" ).withSetting( oldSetting, "booh" )
                .withConfigClasses( asList( mySettingsWithDefaults, myMigratingSettings, new GraphDatabaseSettings() ) )
                .build();

        // When
        config.setLogger( log );

        // Then
        verify( log ).warn( "%s is deprecated. Replaced by %s", oldHello.name(), hello.name() );
        verify( log ).warn( "%s is deprecated.", oldSetting.name() );
        verifyNoMoreInteractions( log );
    }

    @Test
    public void shouldSetInternalParameter()
    {
        // Given
        Config config = builder().withSetting( secretSetting, "false" ).withSetting( hello, "ABC" )
                .withConfigClasses( asList( mySettingsWithDefaults, myMigratingSettings ) ).build();

        // Then
        assertTrue( config.getConfigValues().get( secretSetting.name() ).internal() );
        assertFalse( config.getConfigValues().get( hello.name() ).internal() );
    }

    @Test
    public void shouldSetDocumentedDefaultValue()
    {
        // Given
        Config config = builder().withSetting( secretSetting, "false" ).withSetting( hello, "ABC" )
                .withConfigClasses( asList( new MySettingsWithDefaults(), myMigratingSettings ) ).build();

        // Then
        assertEquals( of( "<documented default value>" ),
                config.getConfigValues().get( secretSetting.name() ).documentedDefaultValue() );
        assertEquals( empty(), config.getConfigValues().get( hello.name() ).documentedDefaultValue() );
    }

    @Test
    public void validatorsShouldBeCalledWhenBuilding()
    {
        // Should not throw
        builder().withSetting( hello, "neo4j" ).withValidator( new HelloHasToBeNeo4jConfigurationValidator() )
                .withConfigClasses( asList( mySettingsWithDefaults, myMigratingSettings ) ).build();

        expect.expect( InvalidSettingException.class );
        expect.expectMessage( "Setting hello has to set to neo4j" );

        // Should throw
        builder().withSetting( hello, "not-neo4j" ).withValidator( new HelloHasToBeNeo4jConfigurationValidator() )
                .withConfigClasses( asList( mySettingsWithDefaults, myMigratingSettings ) ).build();
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

        Config config = fromFile( confFile ).withSetting( strict_config_validation, "false" )
                .withSetting( "a.b.c.first.jibberish", "baah" ).withSetting( "a.b.c.second.jibberish", "baah" )
                .withSetting( "a.b.c.third.jibberish", "baah" ).withSetting( "a.b.c.forth.jibberish", "baah" ).build();

        Set<String> identifiers = config.identifiersFromGroup( GroupedSetting.class );
        Set<String> expectedIdentifiers = new HashSet<>( asList( "first", "second", "third", "forth" ) );

        assertEquals( expectedIdentifiers, identifiers );
    }

    @Test
    public void isConfigured()
    {
        Config config = Config();
        assertFalse( config.isConfigured( hello ) );
        config.augment( hello, "Hi" );
        assertTrue( config.isConfigured( hello ) );
    }

    @Test
    public void isConfiguredShouldNotReturnTrueEvenThoughDefaultValueExists()
    {
        Config config = Config();
        assertFalse( config.isConfigured( hello ) );
        assertEquals( "Hello, World!", config.get( hello ) );
    }

    @Test
    public void withConnectorsDisabled()
    {
        Connector httpConnector = new HttpConnector();
        Connector boltConnector = new BoltConnector();
        Config config = builder().withSetting( httpConnector.enabled, "true" ).withSetting( httpConnector.type, HTTP.name() )
                .withSetting( boltConnector.enabled, "true" ).withSetting( boltConnector.type, BOLT.name() ).withConnectorsDisabled().build();
        assertFalse( config.get( httpConnector.enabled ) );
        assertFalse( config.get( boltConnector.enabled ) );
    }

    @Test
    public void augmentDefaults()
    {
        Config config = Config();
        assertEquals( "Hello, World!", config.get( hello ) );
        config.augmentDefaults( hello, "new default" );
        assertEquals( "new default", config.get( hello ) );
    }

    public static class MyDynamicSettings implements LoadableConfig
    {
        @Dynamic
        public static final Setting<Boolean> boolSetting = setting( "bool_setting", BOOLEAN, TRUE );
    }

    @Test
    public void updateDynamicShouldLogChanges()
    {
        String settingName = MyDynamicSettings.boolSetting.name();
        String changedMessage = "Setting changed: '%s' changed from '%s' to '%s' via '%s'";
        Config config = builder().withConfigClasses( singletonList( new MyDynamicSettings() ) ).build();

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
    public void updateDynamicShouldThrowIfSettingIsNotDynamic()
    {
        Config config = builder().withConfigClasses( singletonList( mySettingsWithDefaults ) ).build();
        expect.expect( IllegalArgumentException.class );
        config.updateDynamicSetting( hello.name(), "hello", ORIGIN );
    }

    @Test
    public void updateDynamicShouldInformRegisteredListeners()
    {
        Config config = builder().withConfigClasses( singletonList( new MyDynamicSettings() ) ).build();
        AtomicInteger counter = new AtomicInteger( 0 );
        config.registerDynamicUpdateListener( MyDynamicSettings.boolSetting, ( previous, update ) -> {
            counter.getAndIncrement();
            assertTrue( previous );
            assertFalse( update );
        } );
        config.updateDynamicSetting( MyDynamicSettings.boolSetting.name(), "false", ORIGIN );
        assertThat( counter.get(), is( 1 ) );
    }

    @Test
    public void updateDynamicShouldNotAllowInvalidSettings()
    {
        Config config = builder().withConfigClasses( singletonList( new MyDynamicSettings() ) ).build();
        expect.expect( InvalidSettingException.class );
        config.updateDynamicSetting( MyDynamicSettings.boolSetting.name(), "this is not a boolean", ORIGIN );
    }

    @Test
    public void registeringUpdateListenerOnNonDynamicSettingMustThrow()
    {
        Config config = builder().withConfigClasses( singletonList( mySettingsWithDefaults ) ).build();
        expect.expect( IllegalArgumentException.class );
        config.registerDynamicUpdateListener( hello, ( a, b ) -> fail( "never called" ) );
    }

    @Test
    public void updateDynamicShouldLogExceptionsFromUpdateListeners()
    {
        Config config = builder().withConfigClasses( singletonList( new MyDynamicSettings() ) ).build();
        IllegalStateException exception = new IllegalStateException( "Boo" );
        config.registerDynamicUpdateListener( MyDynamicSettings.boolSetting, ( a, b ) -> {
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

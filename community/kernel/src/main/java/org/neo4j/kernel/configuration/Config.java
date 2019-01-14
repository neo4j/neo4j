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
package org.neo4j.kernel.configuration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.neo4j.configuration.ConfigOptions;
import org.neo4j.configuration.ConfigValue;
import org.neo4j.configuration.LoadableConfig;
import org.neo4j.configuration.Secret;
import org.neo4j.graphdb.config.BaseSetting;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.config.SettingGroup;
import org.neo4j.graphdb.config.SettingValidator;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.HttpConnector.Encryption;
import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;
import org.neo4j.kernel.info.DiagnosticsPhase;
import org.neo4j.kernel.info.DiagnosticsProvider;
import org.neo4j.logging.BufferingLog;
import org.neo4j.logging.Log;
import org.neo4j.logging.Logger;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonMap;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.configuration.Connector.ConnectorType.BOLT;
import static org.neo4j.kernel.configuration.Connector.ConnectorType.HTTP;
import static org.neo4j.kernel.configuration.HttpConnector.Encryption.NONE;
import static org.neo4j.kernel.configuration.HttpConnector.Encryption.TLS;
import static org.neo4j.kernel.configuration.Settings.TRUE;

/**
 * This class holds the overall configuration of a Neo4j database instance. Use the accessors to convert the internal
 * key-value settings to other types.
 * <p>
 * Users can assume that old settings have been migrated to their new counterparts, and that defaults have been
 * applied.
 */
public class Config implements DiagnosticsProvider, Configuration
{
    public static final String DEFAULT_CONFIG_FILE_NAME = "neo4j.conf";

    private final List<ConfigOptions> configOptions;

    private final Map<String,String> params = new CopyOnWriteHashMap<>(); // Read heavy workload
    private final Map<String, Collection<BiConsumer<String,String>>> updateListeners = new ConcurrentHashMap<>();
    private final ConfigurationMigrator migrator;
    private final List<ConfigurationValidator> validators = new ArrayList<>();
    private final Map<String,String> overriddenDefaults = new CopyOnWriteHashMap<>();
    private final Map<String,BaseSetting<?>> settingsMap; // Only contains fixed settings and not groups

    // Messages to this log get replayed into a real logger once logging has been instantiated.
    private Log log = new BufferingLog();

    /**
     * Builder class for a configuration.
     * <p>
     * The configuration has three layers of values:
     * <ol>
     *   <li>Defaults settings, which is provided by validators.
     *   <li>File settings, parsed from the configuration file if one is provided.
     *   <li>Overridden settings, as provided by the user with the {@link Builder#withSettings(Map)} methods.
     * </ol>
     * They are added in the order specified, and is thus overridden by each layer.
     * <p>
     * Although the builder allows you to override the {@link LoadableConfig}'s with <code>withConfigClasses</code>,
     * this functionality is mainly for testing. If no classes are provided to the builder, they will be located through
     * service loading, and this is probably what you want in most of the cases.
     * <p>
     * Loaded {@link LoadableConfig}'s, whether provided though service loading or explicitly passed, will be scanned
     * for validators that provides migration, validation and default values. Migrators can be specified with the
     * {@link Migrator} annotation and should reside in a class implementing {@link LoadableConfig}.
     */
    public static class Builder
    {
        private Map<String,String> initialSettings = stringMap();
        private Map<String,String> overriddenDefaults = stringMap();
        private List<ConfigurationValidator> validators = new ArrayList<>();
        private File configFile;
        private List<LoadableConfig> settingsClasses;
        private boolean connectorsDisabled;

        /**
         * Augment the configuration with the passed setting.
         *
         * @param setting The setting to set.
         * @param value The value of the setting, pre parsed.
         */
        public Builder withSetting( final Setting<?> setting, final String value )
        {
            return withSetting( setting.name(), value );
        }

        /**
         * Augment the configuration with the passed setting.
         *
         * @param setting The setting to set.
         * @param value The value of the setting, pre parsed.
         */
        public Builder withSetting( final String setting, final String value )
        {
            initialSettings.put( setting, value );
            return this;
        }

        /**
         * Augment the configuration with the passed settings.
         *
         * @param initialSettings settings to augment the configuration with.
         */
        public Builder withSettings( final Map<String,String> initialSettings )
        {
            this.initialSettings.putAll( initialSettings );
            return this;
        }

        /**
         * Set the classes that contains the {@link Setting} fields. If no classes are provided to the builder, they
         * will be located through service loading.
         *
         * @param loadableConfigs A collection fo class instances providing settings.
         */
        @Nonnull
        public Builder withConfigClasses( final Collection<? extends LoadableConfig> loadableConfigs )
        {
            if ( settingsClasses == null )
            {
                settingsClasses = new ArrayList<>();
            }
            settingsClasses.addAll( loadableConfigs );
            return this;
        }

        /**
         * Provide an additional validator. Validators are automatically localed within classes with
         * {@link LoadableConfig}, but this allows you to add others.
         *
         * @param validator an additional validator.
         */
        @Nonnull
        public Builder withValidator( final ConfigurationValidator validator )
        {
            this.validators.add( validator );
            return this;
        }

        /**
         * @see Builder#withValidator(ConfigurationValidator)
         */
        @Nonnull
        public Builder withValidators( final Collection<ConfigurationValidator> validators )
        {
            this.validators.addAll( validators );
            return this;
        }

        /**
         * Extends config with defaults for server, i.e. auth and connector settings.
         */
        @Nonnull
        public Builder withServerDefaults()
        {
            // Add server defaults
            HttpConnector http = new HttpConnector( "http", NONE );
            HttpConnector https = new HttpConnector( "https", TLS );
            BoltConnector bolt = new BoltConnector( "bolt" );
            overriddenDefaults.put( GraphDatabaseSettings.auth_enabled.name(), TRUE );
            overriddenDefaults.put( http.enabled.name(), TRUE );
            overriddenDefaults.put( https.enabled.name(), TRUE );
            overriddenDefaults.put( bolt.enabled.name(), TRUE );

            // Add server validator
            validators.add( new ServerConfigurationValidator() );

            return this;
        }

        /**
         * Provide a file for initial configuration. The settings added with the {@link Builder#withSettings(Map)}
         * methods will be applied on top of the settings in the file.
         *
         * @param configFile A configuration file to parse for initial settings.
         */
        @Nonnull
        public Builder withFile( final @Nullable File configFile )
        {
            this.configFile = configFile;
            return this;
        }

        /**
         * @see Builder#withFile(File)
         */
        @Nonnull
        public Builder withFile( final Path configFile )
        {
            return withFile( configFile.toFile() );
        }

        /**
         * @param configFile an optional configuration file. If not present, this call changes nothing.
         */
        @Nonnull
        public Builder withFile( Optional<File> configFile )
        {
            configFile.ifPresent( file -> this.configFile = file );
            return this;
        }

        /**
         * Specifies the neo4j home directory to be set for this particular config. This will modify {@link
         * GraphDatabaseSettings#neo4j_home} to the same value as provided. If this is not called, the home directory
         * will be set to a system specific default home directory.
         *
         * @param homeDir The home directory this config belongs to.
         */
        @Nonnull
        public Builder withHome( final File homeDir )
        {
            initialSettings.put( GraphDatabaseSettings.neo4j_home.name(), homeDir.getAbsolutePath() );
            return this;
        }

        /**
         * @see Builder#withHome(File)
         */
        @Nonnull
        public Builder withHome( final Path homeDir )
        {
            return withHome( homeDir.toFile() );
        }

        /**
         * This will force all connectors to be disabled during creation of the config. This can be useful if an
         * offline mode is wanted, e.g. in dbms tools or test environments.
         */
        @Nonnull
        public Builder withConnectorsDisabled()
        {
            connectorsDisabled = true;
            return this;
        }

        /**
         * @return The config reflecting the state of the builder.
         * @throws InvalidSettingException is thrown if an invalid setting is encountered and {@link
         * GraphDatabaseSettings#strict_config_validation} is true.
         */
        @Nonnull
        public Config build() throws InvalidSettingException
        {
            List<LoadableConfig> loadableConfigs =
                    Optional.ofNullable( settingsClasses ).orElseGet( LoadableConfig::allConfigClasses );

            // If reading from a file, make sure we always have a neo4j_home
            if ( configFile != null && !initialSettings.containsKey( GraphDatabaseSettings.neo4j_home.name() ) )
            {
                initialSettings.put( GraphDatabaseSettings.neo4j_home.name(), System.getProperty( "user.dir" ) );
            }

            Config config = new Config( configFile, initialSettings, overriddenDefaults, validators, loadableConfigs );

            if ( connectorsDisabled )
            {
                config.augment( config.allConnectorIdentifiers().stream().collect(
                        Collectors.toMap( id -> new Connector( id ).enabled.name(), id -> Settings.FALSE ) ) );
            }

            return config;
        }
    }

    @Nonnull
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * Convenient method for starting building from a file.
     */
    @Nonnull
    public static Builder fromFile( @Nullable final File configFile )
    {
        return builder().withFile( configFile );
    }

    /**
     * Convenient method for starting building from a file.
     */
    @Nonnull
    public static Builder fromFile( @Nonnull final Path configFile )
    {
        return builder().withFile( configFile );
    }

    /**
     * Convenient method for starting building from initial settings.
     */
    @Nonnull
    public static Builder fromSettings( final Map<String,String> initialSettings )
    {
        return builder().withSettings( initialSettings );
    }

    /**
     * @return a configuration with default values.
     */
    @Nonnull
    public static Config defaults()
    {
        return builder().build();
    }

    /**
     * @param initialSettings a map with settings to be present in the config.
     * @return a configuration with default values augmented with the provided <code>initialSettings</code>.
     */
    @Nonnull
    public static Config defaults( @Nonnull final Map<String,String> initialSettings )
    {
        return builder().withSettings( initialSettings ).build();
    }

    /**
     * Constructs a <code>Config</code> with default values and sets the supplied <code>setting</code> to the <code>value</code>.
     * @param setting The initial setting to use.
     * @param value The initial value to give the setting.
     */
    @Nonnull
    public static Config defaults( @Nonnull final Setting<?> setting, final String value )
    {
        return builder().withSetting( setting, value ).build();
    }

    private Config( File configFile,
            Map<String,String> initialSettings,
            Map<String,String> overriddenDefaults,
            Collection<ConfigurationValidator> additionalValidators,
            List<LoadableConfig> settingsClasses )
    {
        configOptions = settingsClasses.stream()
                .map( LoadableConfig::getConfigOptions )
                .flatMap( List::stream )
                .collect( Collectors.toList() );

        settingsMap = new HashMap<>();
        configOptions.stream()
                .map( ConfigOptions::settingGroup )
                .filter( BaseSetting.class::isInstance )
                .map( BaseSetting.class::cast )
                .forEach( setting -> settingsMap.put( setting.name(), setting ) );

        validators.addAll( additionalValidators );
        migrator = new AnnotationBasedConfigurationMigrator( settingsClasses );
        this.overriddenDefaults.putAll( overriddenDefaults );

        boolean fromFile = configFile != null;
        if ( fromFile )
        {
            loadFromFile( configFile, log ).forEach( initialSettings::putIfAbsent );
        }

        overriddenDefaults.forEach( initialSettings::putIfAbsent );

        migrateAndValidateAndUpdateSettings( initialSettings, fromFile );

        // Only warn for deprecations if red from a file
        if ( fromFile )
        {
            warnAboutDeprecations( params );
        }
    }

    /**
     * Retrieves a configuration value. If no value is configured, a default value will be returned instead. Note that
     * {@code null} is a valid value.
     *
     * @param setting The configuration property.
     * @param <T> the underlying type of the setting.
     * @return the value of the given setting, {@code null} can be returned.
     */
    @Override
    public <T> T get( Setting<T> setting )
    {
        return setting.apply( params::get );
    }

    /**
     * Test whether a setting is configured or not. Can be used to check if default value will be returned or not.
     *
     * @param setting The setting to check.
     * @return {@code true} if the setting is configures, {@code false} otherwise implying that the default value will
     * be returned if applicable.
     */
    public boolean isConfigured( Setting<?> setting )
    {
        return params.containsKey( setting.name() );
    }

    /**
     * Returns the currently configured identifiers for grouped settings.
     *
     * Identifiers for groups exists to allow multiple configured settings of the same setting type.
     * E.g. giving that prefix of a group is {@code dbms.ssl.policy} and the following settings are configured:
     * <ul>
     * <li> {@code dbms.ssl.policy.default.base_directory}
     * <li> {@code dbms.ssl.policy.other.base_directory}
     * </ul>
     * a call to this will method return {@code ["default", "other"]}.
     * <p>
     * The key difference to these identifiers are that they are only known at runtime after a valid configuration is
     * parsed and validated.
     *
     * @param groupClass A class that represents a setting group. Must be annotated with {@link Group}
     * @return A set of configured identifiers for the given group.
     * @throws IllegalArgumentException if the provided class is not annotated with {@link Group}.
     */
    public Set<String> identifiersFromGroup( Class<?> groupClass )
    {
        if ( !groupClass.isAnnotationPresent( Group.class ) )
        {
            throw new IllegalArgumentException( "Class must be annotated with @Group" );
        }

        String prefix = groupClass.getAnnotation( Group.class ).value();
        Pattern pattern = Pattern.compile( Pattern.quote( prefix ) + "\\.([^.]+)\\.(.+)" );

        Set<String> identifiers = new TreeSet<>();
        for ( String setting : params.keySet() )
        {
            Matcher matcher = pattern.matcher( setting );
            if ( matcher.matches() )
            {
                identifiers.add( matcher.group( 1 ) );
            }
        }
        return identifiers;
    }

    /**
     * Augment the existing config with new settings, overriding any conflicting settings, but keeping all old
     * non-overlapping ones.
     *
     * @param settings to add and override.
     * @throws InvalidSettingException when and invalid setting is found and {@link
     * GraphDatabaseSettings#strict_config_validation} is true.
     */
    public void augment( Map<String,String> settings ) throws InvalidSettingException
    {
        migrateAndValidateAndUpdateSettings( settings, false );
    }

    /**
     * @see Config#augment(Map)
     */
    public void augment( String setting, String value ) throws InvalidSettingException
    {
        augment( singletonMap( setting, value ) );
    }

    /**
     * @see Config#augment(Map)
     */
    public void augment( Setting<?> setting, String value )
    {
        augment( setting.name(), value );
    }

    /**
     * Augment the existing config with new settings, overriding any conflicting settings, but keeping all old
     * non-overlapping ones.
     *
     * @param config config to add and override with.
     * @throws InvalidSettingException when and invalid setting is found and {@link
     * GraphDatabaseSettings#strict_config_validation} is true.
     */
    public void augment( Config config ) throws InvalidSettingException
    {
        augment( config.params );
    }

    /**
     * Augment the existing config with new settings, ignoring any conflicting settings.
     *
     * @param setting settings to add and override
     * @throws InvalidSettingException when and invalid setting is found and {@link
     * GraphDatabaseSettings#strict_config_validation} is true.
     */
    public void augmentDefaults( Setting<?> setting, String value ) throws InvalidSettingException
    {
        overriddenDefaults.put( setting.name(), value );
        params.putIfAbsent( setting.name(), value );
    }

    /**
     * Specify a log where errors and warnings will be reported. Log messages that happens prior to setting a logger
     * will be buffered and replayed onto the first logger that is set.
     *
     * @param log to use.
     */
    public void setLogger( Log log )
    {
        if ( this.log instanceof BufferingLog )
        {
            ((BufferingLog) this.log).replayInto( log );
        }
        this.log = log;
    }

    /**
     * @param key to lookup in the config
     * @return the value or none if it doesn't exist in the config
     */
    public Optional<String> getRaw( @Nonnull String key )
    {
        return Optional.ofNullable( params.get( key ) );
    }

    /**
     * @return a copy of the raw configuration map
     */
    public Map<String,String> getRaw()
    {
        return new HashMap<>( params );
    }

    /**
     * @return a configured setting
     */
    public Optional<Object> getValue( @Nonnull String key )
    {
        return configOptions.stream()
                .map( it -> it.asConfigValues( params ) )
                .flatMap( List::stream )
                .filter( it -> it.name().equals( key ) )
                .map( ConfigValue::value )
                .findFirst()
                .orElseGet( Optional::empty );
    }

    /**
     * Updates a provided setting to a given value. This method is intended to be used for changing settings during
     * runtime. If you want to change settings at startup, use {@link Config#augment}.
     *
     * @implNote No migration or config validation is done. If you need this you have to refactor this method.
     *
     * @param setting The setting to set to the specified value.
     * @param update The new value to set, passing {@code null} or the empty string should reset the value back to default value.
     * @param origin The source of the change, e.g. {@code dbms.setConfigValue()}.
     * @throws IllegalArgumentException if the provided setting is unknown or not dynamic.
     * @throws InvalidSettingException if the value is not formatted correctly.
     */
    public void updateDynamicSetting( String setting, String update, String origin )
            throws IllegalArgumentException, InvalidSettingException
    {
        verifyValidDynamicSetting( setting );

        synchronized ( params )
        {
            boolean oldValueIsDefault = false;
            boolean newValueIsDefault = false;
            String oldValue;
            String newValue;
            if ( update == null || update.isEmpty() )
            {
                // Empty means we want to delete the configured value and fallback to the default value
                String overriddenDefault = overriddenDefaults.get( setting );
                boolean hasDefault = overriddenDefault != null;
                oldValue = hasDefault ? params.put( setting, overriddenDefault ) : params.remove( setting );
                newValue = getDefaultValueOf( setting );
                newValueIsDefault = true;
            }
            else
            {
                // Change setting, make sure it's valid
                Map<String,String> newEntry = stringMap( setting, update );
                List<SettingValidator> settingValidators = configOptions.stream()
                                                                        .map( ConfigOptions::settingGroup )
                                                                        .collect( Collectors.toList() );
                for ( SettingValidator validator : settingValidators )
                {
                    validator.validate( newEntry, ignore -> {} ); // Throws if invalid
                }

                String previousValue = params.put( setting, update );
                if ( previousValue != null )
                {
                    oldValue = previousValue;
                }
                else
                {
                    oldValue = getDefaultValueOf( setting );
                    oldValueIsDefault = true;
                }
                newValue = update;
            }

            String oldValueForLog = obsfucateIfSecret( setting, oldValue );
            String newValueForLog = obsfucateIfSecret( setting, newValue );
            log.info( "Setting changed: '%s' changed from '%s' to '%s' via '%s'",
                    setting, oldValueIsDefault ? "default (" + oldValueForLog + ")" : oldValueForLog,
                    newValueIsDefault ? "default (" + newValueForLog + ")" : newValueForLog, origin );
            updateListeners.getOrDefault( setting, emptyList() ).forEach( l -> l.accept( oldValue, newValue ) );
        }
    }

    private void verifyValidDynamicSetting( String setting )
    {
        Optional<ConfigValue> option = findConfigValue( setting );

        if ( !option.isPresent() )
        {
            throw new IllegalArgumentException( "Unknown setting: " + setting );
        }

        ConfigValue configValue = option.get();
        if ( !configValue.dynamic() )
        {
            throw new IllegalArgumentException( "Setting is not dynamic and can not be changed at runtime" );
        }
    }

    private String getDefaultValueOf( String setting )
    {
        if ( overriddenDefaults.containsKey( setting ) )
        {
            return overriddenDefaults.get( setting );
        }
        if ( settingsMap.containsKey( setting ) )
        {
            return settingsMap.get( setting ).getDefaultValue();
        }
        return "<no default>";
    }

    private Optional<ConfigValue> findConfigValue( String setting )
    {
        return configOptions.stream().map( it -> it.asConfigValues( params ) ).flatMap( List::stream )
                .filter( it -> it.name().equals( setting ) ).findFirst();
    }

    /**
     * Register a listener for dynamic updates to the given setting.
     * <p>
     * The listener will get called whenever the {@link #updateDynamicSetting(String, String, String)} method is used
     * to change the given setting, and the listener will be supplied the parsed values of the old and the new
     * configuration value.
     *
     * @param setting The {@link Setting} to listen for changes to.
     * @param listener The listener callback that will be notified of any configuration changes to the given setting.
     * @param <V> The value type of the setting.
     */
    public <V> void registerDynamicUpdateListener( Setting<V> setting, BiConsumer<V,V> listener )
    {
        String settingName = setting.name();
        verifyValidDynamicSetting( settingName );
        BiConsumer<String,String> projectedListener = ( oldValStr, newValStr ) ->
        {
            try
            {
                V oldVal = setting.apply( s -> oldValStr );
                V newVal = setting.apply( s -> newValStr );
                listener.accept( oldVal, newVal );
            }
            catch ( Exception e )
            {
                log.error( "Failure when notifying listeners after dynamic setting change; " +
                           "new setting might not have taken effect: " + e.getMessage(), e );
            }
        };
        updateListeners.computeIfAbsent( settingName, k -> new ConcurrentLinkedQueue<>() ).add( projectedListener );
    }

    /**
     * @return all effective config values
     */
    public Map<String,ConfigValue> getConfigValues()
    {
        return configOptions.stream()
                .map( it -> it.asConfigValues( params ) )
                .flatMap( List::stream )
                .collect( Collectors.toMap( ConfigValue::name, it -> it, ( val1, val2 ) ->
                {
                    throw new RuntimeException( "Duplicate setting: " + val1.name() + ": " + val1 + " and " + val2 );
                } ) );
    }

    @Override
    public String getDiagnosticsIdentifier()
    {
        return getClass().getName();
    }

    @Override
    public void acceptDiagnosticsVisitor( Object visitor )
    {
        // nothing visits configuration
    }

    @Override
    public void dump( DiagnosticsPhase phase, Logger logger )
    {
        if ( phase.isInitialization() || phase.isExplicitlyRequested() )
        {
            logger.log( "Neo4j Kernel properties:" );
            for ( Map.Entry<String,String> param : params.entrySet() )
            {
                logger.log( "%s=%s", param.getKey(), obsfucateIfSecret( param ) );
            }
        }
    }

    private String obsfucateIfSecret( Map.Entry<String,String> param )
    {
        return obsfucateIfSecret( param.getKey(), param.getValue() );
    }

    private String obsfucateIfSecret( String key, String value )
    {
        if ( settingsMap.containsKey( key ) && settingsMap.get( key ).secret() )
        {
            return Secret.OBSFUCATED;
        }
        else
        {
            return value;
        }
    }

    /**
     * Migrates and validates all string values in the provided <code>settings</code> map.
     *
     * This will update the configuration with the provided values regardless whether errors are encountered or not.
     *
     * @param settings the settings to migrate and validate.
     * @param warnOnUnknownSettings if true method log messages to {@link Config#log}.
     * @throws InvalidSettingException when and invalid setting is found and {@link
     * GraphDatabaseSettings#strict_config_validation} is true.
     */
    private void migrateAndValidateAndUpdateSettings( Map<String,String> settings, boolean warnOnUnknownSettings )
            throws InvalidSettingException
    {
        Map<String,String> migratedSettings = migrateSettings( settings );
        params.putAll( migratedSettings );

        List<SettingValidator> settingValidators = configOptions.stream()
                .map( ConfigOptions::settingGroup )
                .collect( Collectors.toList() );

        // Validate settings
        Map<String,String> additionalSettings =
                new IndividualSettingsValidator( settingValidators, warnOnUnknownSettings ).validate( this, log );
        params.putAll( additionalSettings );

        // Validate configuration
        for ( ConfigurationValidator validator : validators )
        {
            validator.validate( this, log );
        }
    }

    private Map<String,String> migrateSettings( Map<String,String> settings )
    {
        return migrator.apply( settings, log );
    }

    private void warnAboutDeprecations( Map<String,String> userSettings )
    {
        configOptions.stream()
                .flatMap( it -> it.asConfigValues( userSettings ).stream() )
                .filter( config -> userSettings.containsKey( config.name() ) && config.deprecated() )
                .forEach( c ->
                {
                    if ( c.replacement().isPresent() )
                    {
                        log.warn( "%s is deprecated. Replaced by %s", c.name(), c.replacement().get() );
                    }
                    else
                    {
                        log.warn( "%s is deprecated.", c.name() );
                    }
                } );
    }

    @Nonnull
    private static Map<String,String> loadFromFile( @Nonnull File file, @Nonnull Log log )
    {
        if ( !file.exists() )
        {
            log.warn( "Config file [%s] does not exist.", file );
            return new HashMap<>();
        }
        try
        {
            return MapUtil.load( file );
        }
        catch ( IOException e )
        {
            log.error( "Unable to load config file [%s]: %s", file, e.getMessage() );
            return new HashMap<>();
        }
    }

    /**
     * @return a list of all connector names like 'http' in 'dbms.connector.http.enabled = true'
     */
    @Nonnull
    public Set<String> allConnectorIdentifiers()
    {
        return allConnectorIdentifiers( params );
    }

    /**
     * @return a list of all connector names like 'http' in 'dbms.connector.http.enabled = true'
     */
    @Nonnull
    public Set<String> allConnectorIdentifiers( @Nonnull Map<String,String> params )
    {
        return identifiersFromGroup( Connector.class );
    }

    /**
     * @return list of all configured bolt connectors
     */
    @Nonnull
    public List<BoltConnector> boltConnectors()
    {
        return boltConnectors( params ).collect( Collectors.toList() );
    }

    /**
     * @return stream of all configured bolt connectors
     */
    @Nonnull
    private Stream<BoltConnector> boltConnectors( @Nonnull Map<String,String> params )
    {
        return allConnectorIdentifiers( params ).stream().map( BoltConnector::new ).filter(
                c -> c.group.groupKey.equalsIgnoreCase( "bolt" ) || BOLT == c.type.apply( params::get ) );
    }

    /**
     * @return list of all configured bolt connectors which are enabled
     */
    @Nonnull
    public List<BoltConnector> enabledBoltConnectors()
    {
        return enabledBoltConnectors( params );
    }

    /**
     * @return list of all configured bolt connectors which are enabled
     */
    @Nonnull
    public List<BoltConnector> enabledBoltConnectors( @Nonnull Map<String,String> params )
    {
        return boltConnectors( params )
                .filter( c -> c.enabled.apply( params::get ) )
                .collect( Collectors.toList() );
    }

    /**
     * @return list of all configured http connectors
     */
    @Nonnull
    public List<HttpConnector> httpConnectors()
    {
        return httpConnectors( params ).collect( Collectors.toList() );
    }

    /**
     * @return stream of all configured http connectors
     */
    @Nonnull
    private Stream<HttpConnector> httpConnectors( @Nonnull Map<String,String> params )
    {
        return allConnectorIdentifiers( params ).stream()
                .map( Connector::new )
                .filter( c -> c.group.groupKey.equalsIgnoreCase( "http" ) ||
                        c.group.groupKey.equalsIgnoreCase( "https" ) ||
                        HTTP == c.type.apply( params::get ) )
                .map( c ->
                {
                    final String name = c.group.groupKey;
                    final Encryption defaultEncryption;
                    switch ( name )
                    {
                    case "https":
                        defaultEncryption = TLS;
                        break;
                    case "http":
                    default:
                        defaultEncryption = NONE;
                        break;
                    }

                    return new HttpConnector( name,
                            HttpConnectorValidator.encryptionSetting( name, defaultEncryption ).apply( params::get ) );
                } );
    }

    /**
     * @return list of all configured http connectors which are enabled
     */
    @Nonnull
    public List<HttpConnector> enabledHttpConnectors()
    {
        return enabledHttpConnectors( params );
    }

    /**
     * @return list of all configured http connectors which are enabled
     */
    @Nonnull
    private List<HttpConnector> enabledHttpConnectors( @Nonnull Map<String,String> params )
    {
        return httpConnectors( params )
                .filter( c -> c.enabled.apply( params::get ) )
                .collect( Collectors.toList() );
    }

    @Override
    public String toString()
    {
        return params.entrySet().stream()
                .sorted( Comparator.comparing( Map.Entry::getKey ) )
                .map( entry -> entry.getKey() + "=" + obsfucateIfSecret( entry ) )
                .collect( Collectors.joining( ", ") );
    }
}

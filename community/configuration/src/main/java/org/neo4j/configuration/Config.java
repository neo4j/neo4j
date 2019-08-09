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

import org.apache.commons.lang3.reflect.FieldUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.logging.BufferingLog;
import org.neo4j.logging.Log;
import org.neo4j.service.Services;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.neo4j.configuration.GraphDatabaseSettings.strict_config_validation;

public class Config implements Configuration
{
    public static final String DEFAULT_CONFIG_FILE_NAME = "neo4j.conf";

    public static class Builder
    {
        private final Collection<Class<? extends SettingsDeclaration>> settingsClasses = new HashSet<>();
        private final Collection<Class<? extends GroupSetting>> groupSettingClasses = new HashSet<>();
        private final Collection<SettingMigrator> settingMigrators = new HashSet<>();
        private final List<Class<? extends GroupSettingValidator>> validators = new ArrayList<>();
        private final Map<String,String> settingValueStrings = new HashMap<>();
        private final Map<String,Object> settingValueObjects = new HashMap<>();
        private final Map<String,Object> overriddenDefaults = new HashMap<>();
        private Config fromConfig;
        private Log log = new BufferingLog();

        private static boolean allowedToLogOverriddenValues( String setting )
        {
            return !Objects.equals( setting, ExternalSettings.additionalJvm.name() );
        }

        private void overrideSettingValue( String setting, Object value )
        {
            String msg = "The '%s' setting is overridden. Setting value changed from '%s' to '%s'.";
            if ( settingValueStrings.containsKey( setting ) && allowedToLogOverriddenValues( setting ) )
            {
                log.warn( msg, setting, settingValueStrings.remove( setting ), value );
            }

            if ( settingValueObjects.containsKey( setting ) )
            {
                log.warn( msg, setting, settingValueObjects.remove( setting ), value );
            }
        }

        private Builder setRaw( String setting, String value )
        {
            overrideSettingValue( setting, value );
            settingValueStrings.put( setting, value );
            return this;
        }

        private Builder set( String setting, Object value )
        {
            overrideSettingValue( setting, value );
            settingValueObjects.put( setting, value );
            return this;
        }

        public Builder setRaw( Map<String,String> settingValues )
        {
            settingValues.forEach( this::setRaw );
            return this;
        }

        public <T> Builder set( Setting<T> setting, T value )
        {
            return set( setting.name(), value );
        }

        public Builder set( Map<Setting<?>,Object> settingValues )
        {
            settingValues.forEach( ( setting, value ) -> set( setting.name(), value )  );
            return this;
        }

        private Builder setDefault( String setting, Object value )
        {
            if ( overriddenDefaults.containsKey( setting ) && allowedToLogOverriddenValues( setting ) )
            {
                log.warn( "The overridden default value of '%s' setting is overridden. Setting value changed from '%s' to '%s'.", setting,
                        overriddenDefaults.get( setting ), value );
            }
            overriddenDefaults.put( setting, value );
            return this;
        }

        public Builder setDefaults( Map<Setting<?>, Object> overriddenDefaults )
        {
            overriddenDefaults.forEach( ( setting, value ) -> setDefault( setting.name(), value )  );
            return this;
        }

        public <T> Builder setDefault( Setting<T> setting, T value )
        {
            return setDefault( setting.name(), value );
        }

        public Builder remove( Setting<?> setting )
        {
            settingValueStrings.remove( setting.name() );
            settingValueObjects.remove( setting.name() );
            return this;
        }

        public Builder removeDefault( Setting<?> setting )
        {
            overriddenDefaults.remove( setting.name() );
            return this;
        }

        Builder addSettingsClass( Class<? extends SettingsDeclaration> settingsClass )
        {
            this.settingsClasses.add( settingsClass );
            return this;
        }

        Builder addGroupSettingClass( Class<? extends GroupSetting> groupSettingClass )
        {
            this.groupSettingClasses.add( groupSettingClass );
            return this;
        }

        public Builder addValidators( List<Class<? extends GroupSettingValidator>> validators )
        {
            this.validators.addAll( validators );
            return this;
        }

        public Builder addValidator( Class<? extends GroupSettingValidator> validator )
        {
            this.validators.add( validator );
            return this;
        }

        public Builder addMigrator( SettingMigrator migrator )
        {
            this.settingMigrators.add( migrator );
            return this;
        }

        public Builder fromConfig( Config config )
        {
            if ( fromConfig != null )
            {
                throw new IllegalArgumentException( "Can only build a config from one other config." );
            }
            fromConfig = config;
            return this;
        }

        public Builder fromFileNoThrow( Path path )
        {
            if ( path != null )
            {
                fromFile( path.toFile(), false );
            }
            return this;
        }

        public Builder fromFileNoThrow( File cfg )
        {
            return fromFile( cfg, false );
        }

        public Builder fromFile( File cfg )
        {
            return fromFile( cfg, true );
        }

        private Builder fromFile( File file, boolean allowThrow )
        {
            if ( file == null || !file.exists() )
            {
                if ( allowThrow )
                {
                    throw new IllegalArgumentException( new IOException( "Config file [" + file + "] does not exist." ) );
                }
                log.warn( "Config file [%s] does not exist.", file );
                return this;
            }

            try
            {
                try ( FileInputStream stream = new FileInputStream( file ) )
                {
                    new Properties()
                    {
                        @Override
                        public synchronized Object put( Object key, Object value )
                        {
                            setRaw( key.toString(), value.toString() );
                            return null;
                        }
                    }.load( stream );
                }
            }
            catch ( IOException e )
            {
                if ( allowThrow )
                {
                    throw new IllegalArgumentException( "Unable to load config file [" + file + "].", e );
                }
                log.error( "Unable to load config file [%s]: %s", file, e.getMessage() );
            }
            return this;
        }

        private Builder()
        {

        }

        public Config build()
        {
            return new Config( settingsClasses, groupSettingClasses, validators, settingMigrators, settingValueStrings, settingValueObjects, overriddenDefaults,
                    fromConfig, log );
        }
    }

    public static Config defaults()
    {
        return defaults( Map.of() );
    }

    public static <T> Config defaults( Setting<T> setting, T value )
    {
        return defaults( Map.of( setting, value ) );
    }

    public static Config defaults( Map<Setting<?>,Object> settingValues )
    {
        return Config.newBuilder().set( settingValues ).build();
    }

    public static Builder newBuilder()
    {
        Builder builder = new Builder();
        Services.loadAll( SettingsDeclaration.class ).forEach( decl -> builder.addSettingsClass( decl.getClass() ) );
        Services.loadAll( GroupSetting.class ).forEach( decl -> builder.addGroupSettingClass( decl.getClass() ) );
        Services.loadAll( SettingMigrator.class ).forEach( builder::addMigrator );

        return builder;
    }

    public static Builder emptyBuilder()
    {
        return new Builder();
    }

    protected final Map<String,Entry<?>> settings = new HashMap<>();
    private final Map<Class<? extends GroupSetting>, Map<String,GroupSetting>> allGroupInstances = new HashMap<>();
    private Log log;

    protected Config()
    {
    }

    private Config( Collection<Class<? extends SettingsDeclaration>> settingsClasses,
            Collection<Class<? extends GroupSetting>> groupSettingClasses,
            List<Class<? extends GroupSettingValidator>> validatorClasses,
            Collection<SettingMigrator> settingMigrators,
            Map<String,String> settingValueStrings,
            Map<String,Object> settingValueObjects,
            Map<String,Object> overriddenDefaultObjects,
            Config fromConfig,
            Log log )
    {
        this.log = log;

        Map<String,String> overriddenDefaultStrings = new HashMap<>();
        try
        {
            settingMigrators.forEach( migrator -> migrator.migrate( settingValueStrings, overriddenDefaultStrings, log )  );
        }
        catch ( RuntimeException e )
        {
            throw new IllegalArgumentException( "Error while migrating settings, please see the exception cause", e );
        }

        Map<String,SettingImpl<?>> definedSettings = getDefinedSettings( settingsClasses );
        Map<String,Class<? extends GroupSetting>> definedGroups = getDefinedGroups( groupSettingClasses );
        HashSet<String> keys = new HashSet<>( definedSettings.keySet() );
        keys.addAll( settingValueStrings.keySet() );
        keys.addAll( settingValueObjects.keySet() );

        List<SettingImpl<?>> newSettings = new ArrayList<>();

        if ( fromConfig != null ) //When building from another config, extract values
        {
            //fromConfig.log is ignored, until different behaviour is expected
            fromConfig.allGroupInstances.forEach( ( cls, fromGroupMap ) -> {
                Map<String, GroupSetting> groupMap = allGroupInstances.computeIfAbsent( cls, k -> new HashMap<>() );
                groupMap.putAll( fromGroupMap );
            } );
            for ( Map.Entry<String,Entry<?>> entry : fromConfig.settings.entrySet() )
            {
                newSettings.add( entry.getValue().setting );
                keys.remove( entry.getKey() );
            }
        }

        boolean strict = strict_config_validation.defaultValue();
        if ( keys.remove( strict_config_validation.name() ) ) //evaluate strict_config_validation setting first, as we need it when validating other settings
        {
            evaluateSetting( strict_config_validation, settingValueStrings, settingValueObjects,
                    fromConfig, overriddenDefaultStrings, overriddenDefaultObjects );
            strict = get( strict_config_validation );
        }

        newSettings.addAll( getActiveSettings( keys, definedGroups, definedSettings, strict ) );

        evaluateSettingValues( newSettings, settingValueStrings, settingValueObjects, overriddenDefaultStrings, overriddenDefaultObjects, fromConfig );

        validateGroupsettings( validatorClasses );
    }

    @SuppressWarnings( "unchecked" )
    private void evaluateSettingValues( Collection<SettingImpl<?>> settingsToEvaluate, Map<String,String> settingValueStrings,
            Map<String,Object> settingValueObjects,Map<String,String> overriddenDefaultStrings, Map<String,Object> overriddenDefaultObjects, Config fromConfig )
    {
        Deque<SettingImpl<?>> newSettings = new LinkedList<>( settingsToEvaluate );
        while ( !newSettings.isEmpty() )
        {
            boolean modified = false;
            SettingImpl<?> last = newSettings.peekLast();
            SettingImpl<Object> setting;
            do
            {
                setting = (SettingImpl<Object>) requireNonNull( newSettings.pollFirst() );

                if ( setting.dependency() != null && !settings.containsKey( setting.dependency().name() ) )
                {
                    //dependency not yet evaluated, put last
                    newSettings.addLast( setting );
                }
                else
                {
                    modified = true;
                    evaluateSetting( setting, settingValueStrings, settingValueObjects, fromConfig, overriddenDefaultStrings, overriddenDefaultObjects );
                }
            }
            while ( setting != last );

            if ( !modified && !newSettings.isEmpty() )
            {
                //Settings left depend on settings not present in this config.
                String unsolvable = newSettings.stream()
                        .map( s -> format("'%s'->'%s'", s.name(), s.dependency().name() ) )
                        .collect( Collectors.joining(",\n","[","]"));
                throw new IllegalArgumentException(
                        format( "Can not resolve setting dependencies. %s depend on settings not present in config, or are in a circular dependency ",
                                unsolvable ) );
            }
        }
    }

    private Collection<SettingImpl<?>> getActiveSettings( Set<String> settingNames, Map<String,Class<? extends GroupSetting>> definedGroups,
            Map<String,SettingImpl<?>> declaredSettings, boolean strict )
    {
        List<SettingImpl<?>> newSettings = new ArrayList<>();
        for ( String key : settingNames )
        {
            // Try to find in settings
            SettingImpl<?> setting = declaredSettings.get( key );
            if ( setting != null )
            {
                newSettings.add( setting );
            }
            else
            {
                // Not found, could be a group setting, e.g "dbms.ssl.policy.*"
                var groupEntryOpt = definedGroups.entrySet().stream().filter( e -> key.startsWith( e.getKey() + '.' ) ).findAny();
                if ( groupEntryOpt.isEmpty() )
                {
                    String msg = format( "Unrecognized setting. No declared setting with name: %s", key );
                    if ( strict )
                    {
                        throw new IllegalArgumentException( msg );
                    }
                    log.warn( msg );
                    continue;
                }
                var groupEntry = groupEntryOpt.get();

                String prefix = groupEntry.getKey();
                String keyWithoutPrefix = key.substring( prefix.length() + 1 );
                String id;
                if ( keyWithoutPrefix.matches("^[^.]+$") )
                {
                    id = keyWithoutPrefix;
                }
                else if ( keyWithoutPrefix.matches("^[^.]+\\.[^.]+$") )
                {
                    id = keyWithoutPrefix.substring( 0, keyWithoutPrefix.indexOf( '.' ) );
                }
                else
                {
                    String msg = format( "Malformed group setting name: '%s', does not match any setting in its group.", key );
                    if ( strict )
                    {
                        throw new IllegalArgumentException( msg );
                    }
                    log.warn( msg );
                    continue;
                }

                Map<String, GroupSetting> groupInstances = allGroupInstances.computeIfAbsent( groupEntry.getValue(), k -> new HashMap<>() );
                if ( !groupInstances.containsKey( id ) )
                {

                    GroupSetting group;
                    try
                    {
                        group = createStringInstance( groupEntry.getValue(), id );
                    }
                    catch ( IllegalArgumentException e )
                    {
                        String msg = format( "Unrecognized setting. No declared setting with name: %s", key );
                        if ( strict )
                        {
                            throw new IllegalArgumentException( msg );
                        }
                        log.warn( msg );
                        continue;
                    }
                    groupInstances.put( id, group );
                    //Add all settings from created groups, to get possible default values.
                    Map<String,SettingImpl<?>> definedSettings = getDefinedSettings( group.getClass(), group );
                    if ( definedSettings.values().stream().anyMatch( SettingImpl::dynamic ) )
                    {
                        throw new IllegalArgumentException( format( "Group setting can not be dynamic: '%s'", key ) );
                    }
                    newSettings.addAll( definedSettings.values() );
                }
            }
        }
        return newSettings;
    }

    private void validateGroupsettings( List<Class<? extends GroupSettingValidator>> validatorClasses )
    {
        for ( GroupSettingValidator validator : getGroupSettingValidators( validatorClasses ) )
        {
            String prefix = validator.getPrefix() + '.';
            Map<Setting<?>, Object> values = settings.entrySet().stream()
                    .filter( e -> e.getKey().startsWith( prefix ) )
                    .collect( HashMap::new, ( map, entry ) -> map.put( entry.getValue().setting, entry.getValue().getValue() ), HashMap::putAll );

            validator.validate( values, this );
        }
    }

    @SuppressWarnings( "unchecked" )
    private void evaluateSetting( Setting<?> untypedSetting, Map<String,String> settingValueStrings, Map<String,Object> settingValueObjects, Config fromConfig,
            Map<String,String> overriddenDefaultStrings, Map<String,Object> overriddenDefaultObjects )
    {
        SettingImpl<Object> setting = (SettingImpl<Object>) untypedSetting;
        String key = setting.name();

        try
        {
            Object defaultValue = null;
            if ( overriddenDefaultObjects.containsKey( key ) ) // Map default value
            {
                defaultValue = overriddenDefaultObjects.get( key );
            }
            else if ( overriddenDefaultStrings.containsKey( key ) )
            {
                defaultValue = setting.parse( overriddenDefaultStrings.get( key ) );
            }
            else
            {
                defaultValue = setting.defaultValue();
                if ( fromConfig != null && fromConfig.settings.containsKey( key ) )
                {
                    Object fromDefault = fromConfig.settings.get( key ).defaultValue;
                    if ( !Objects.equals( defaultValue, fromDefault ) )
                    {
                        defaultValue = fromDefault;
                    }
                }
            }

            Object value = null;
            if ( settingValueObjects.containsKey( key ) )
            {
                value = settingValueObjects.get( key );

            }
            else if ( settingValueStrings.containsKey( key ) ) // Map value
            {
                value = setting.parse( settingValueStrings.get( key ) );
            }
            else if ( fromConfig != null && fromConfig.settings.containsKey( key ) )
            {
                Entry<?> entry = fromConfig.settings.get( key );
                value = entry.isDefault ? null : entry.value;
            }

            value = setting.solveDefault( value, defaultValue );

            settings.put( key, createEntry( setting, value, defaultValue ) );
        }
        catch ( RuntimeException exception )
        {
            String msg = format( "Error evaluating value for setting '%s'. %s", setting.name(), exception.getMessage() );
            throw new IllegalArgumentException( msg, exception );
        }
    }

    @SuppressWarnings( "unchecked" )
    private <T> Entry<T> createEntry( SettingImpl<T> setting, T value, T defaultValue )
    {
        if ( setting.dependency() != null )
        {
            var dep = settings.get( setting.dependency().name() );
            T solvedValue = setting.solveDependency( value != null ? value : defaultValue, (T) dep.getValue() );
            //can not validate default value when we have dependency, as it is not solved itself, but included in the solved value
            setting.validate( solvedValue );
            return new DepEntry<>( setting, value, defaultValue, solvedValue );
        }
        setting.validate( defaultValue );
        setting.validate( value );
        return new Entry<>( setting, value, defaultValue );
    }

    @SuppressWarnings( "unchecked" )
    public <T extends GroupSetting> Map<String,T> getGroups( Class<T> group )
    {
        return new HashMap<>( (Map<? extends String,? extends T>) allGroupInstances.getOrDefault( group, new HashMap<>() ) );
    }

    @SuppressWarnings( "unchecked" )
    public <T extends GroupSetting, U extends T> Map<Class<U>,Map<String,U>> getGroupsFromInheritance( Class<T> parentClass )
    {
        return allGroupInstances.keySet().stream()
                .filter( parentClass::isAssignableFrom )
                .map( childClass -> (Class<U>) childClass )
                .collect( Collectors.toMap( childClass -> childClass, this::getGroups ) );
    }

    private static List<GroupSettingValidator> getGroupSettingValidators( List<Class<? extends GroupSettingValidator>> validatorClasses )
    {
        List<GroupSettingValidator> validators = new ArrayList<>();
        validatorClasses.forEach( validatorClass -> validators.add( createInstance( validatorClass ) ) );
        return validators;
    }

    private static <T> T createInstance( Class<T> classObj )
    {

        T instance;
        try
        {
            instance = createStringInstance( classObj, null );
        }
        catch ( Exception first )
        {
            try
            {
                Constructor<T> constructor = classObj.getDeclaredConstructor();
                constructor.setAccessible( true );
                instance = constructor.newInstance();
            }
            catch ( Exception second )
            {
                String name = classObj.getSimpleName();
                String msg = format( "Failed to create instance of: %s, please see the exception cause", name );
                throw new IllegalArgumentException( msg, Exceptions.chain( second, first ) );
            }

        }
        return instance;
    }

    @Override
    public <T> T get( org.neo4j.graphdb.config.Setting<T> setting )
    {
        return getObserver( setting ).getValue();
    }

    @SuppressWarnings( "unchecked" )
    public <T> SettingObserver<T> getObserver( Setting<T> setting )
    {
        SettingObserver<T> observer = (SettingObserver<T>) settings.get( setting.name() );
        if ( observer != null )
        {
            return observer;
        }
        throw new IllegalArgumentException( format( "Config has no association with setting: '%s'", setting.name() ) );
    }

    public <T> void setDynamic( Setting<T> setting, T value, String scope )
    {
        Entry<T> entry = (Entry<T>) getObserver( setting );
        SettingImpl<T> actualSetting = entry.setting;
        if ( !actualSetting.dynamic() )
        {
            throw new IllegalArgumentException( format("Setting '%s' is not dynamic and can not be changed at runtime", setting.name() ) );
        }
        set( setting, value );
        log.info( "%s changed to %s, by %s", setting.name(), actualSetting.valueToString( value ), scope );

    }

    public <T> void set( Setting<T> setting, T value )
    {
        Entry<T> entry = (Entry<T>) getObserver( setting );
        SettingImpl<T> actualSetting = entry.setting;
        if ( actualSetting.immutable() )
        {
            throw new IllegalArgumentException( format("Setting '%s' immutable (final). Can not amend", actualSetting.name() ) );
        }
        entry.setValue( value );
    }

    public <T> void setIfNotSet( Setting<T> setting, T value )
    {
        Entry<T> entry = (Entry<T>) getObserver( setting );
        if ( entry == null || entry.isDefault )
        {
            set( setting, value );
        }
    }

    public boolean isExplicitlySet( Setting<?> setting )
    {
        if ( settings.containsKey( setting.name() ) )
        {
            return !settings.get( setting.name() ).isDefault;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return toString( true );
    }

    @SuppressWarnings( "unchecked" )
    public String toString( boolean includeNullValues )
    {
        StringBuilder sb = new StringBuilder();
        settings.entrySet().stream()
                .sorted( Map.Entry.comparingByKey() )
                .forEachOrdered( e ->
                {
                    SettingImpl<Object> setting = (SettingImpl<Object>) e.getValue().setting;
                    Object valueObj = e.getValue().getValue();
                    if ( valueObj != null || includeNullValues )
                    {
                        String value = setting.valueToString( valueObj );
                        sb.append( format( "%s=%s%n", e.getKey(), value ) );
                    }
                } );
        return sb.toString();
    }

    public void setLogger( Log log )
    {
        if ( this.log instanceof BufferingLog )
        {
            ((BufferingLog) this.log).replayInto( log );
        }
        this.log = log;
    }

    @SuppressWarnings( "unchecked" )
    public Map<Setting<Object>,Object> getValues()
    {
        HashMap<Setting<Object>,Object> values = new HashMap<>();
        settings.forEach( ( s, entry ) -> values.put( (Setting<Object>) entry.setting, entry.value ) );
        return values;
    }

    @SuppressWarnings( "unchecked" )
    public Setting<Object> getSetting( String name )
    {
        if ( !settings.containsKey( name ) )
        {
            throw new IllegalArgumentException( format( "Setting `%s` not found", name ) );
        }
        return (Setting<Object>) settings.get( name ).setting;
    }
    @SuppressWarnings( "unchecked" )
    public Map<String,Setting<Object>> getDeclaredSettings()
    {
        return settings.entrySet().stream().collect( Collectors.toMap( Map.Entry::getKey, entry -> (Setting<Object>) entry.getValue().setting ) );
    }

    private static Map<String,Class<? extends GroupSetting>> getDefinedGroups( Collection<Class<? extends GroupSetting>> groupSettingClasses )
    {
        return groupSettingClasses.stream().collect( Collectors.toMap( cls -> createInstance( cls ).getPrefix(), cls -> cls ) );
    }

    private static <T> T createStringInstance( Class<T> cls, String id )
    {
        try
        {
            Constructor<T> constructor = cls.getDeclaredConstructor( String.class );
            constructor.setAccessible( true );
            return constructor.newInstance( id );
        }
        catch ( Exception e )
        {
            if ( e.getCause() instanceof  IllegalArgumentException )
            {
                throw new IllegalArgumentException( "Could not create instance with id: " + id, e );
            }
            String msg = format( "'%s' must have a ( @Nullable String ) constructor, be static & non-abstract", cls.getSimpleName() );
            throw new RuntimeException( msg, e );
        }
    }

    private static Map<String,SettingImpl<?>> getDefinedSettings( Collection<Class<? extends SettingsDeclaration>> settingsClasses )
    {
        Map<String,SettingImpl<?>> settings = new HashMap<>();
        settingsClasses.forEach( c -> settings.putAll( getDefinedSettings( c, null ) ) );
        return settings;
    }

    private static Map<String,SettingImpl<?>> getDefinedSettings( Class<?> settingClass, Object fromObject )
    {
        Map<String,SettingImpl<?>> settings = new HashMap<>();
        Arrays.stream( FieldUtils.getAllFields( settingClass ) )
                .filter( f -> f.getType().isAssignableFrom( SettingImpl.class ) )
                .forEach( field ->
                {
                    try
                    {
                        field.setAccessible( true );
                        SettingImpl<?> setting = (SettingImpl<?>) field.get( fromObject );
                        if ( field.isAnnotationPresent( Description.class ) )
                        {
                            setting.setDescription( field.getAnnotation( Description.class ).value() );
                        }
                        if ( field.isAnnotationPresent( DocumentedDefaultValue.class ) )
                        {
                            setting.setDocumentedDefaultValue( field.getAnnotation( DocumentedDefaultValue.class ).value() );
                        }
                        if ( field.isAnnotationPresent( Internal.class ) )
                        {
                            setting.setInternal();
                        }
                        if ( field.isAnnotationPresent( Deprecated.class ) )
                        {
                            setting.setDeprecated();
                        }
                        settings.put( setting.name(), setting );
                    }
                    catch ( Exception e )
                    {
                        throw new RuntimeException( format( "%s %s, from %s is not accessible.", field.getType(), field.getName(),
                                settingClass.getSimpleName() ), e );
                    }
                } );
        return settings;
    }

    public <T> void addListener( Setting<T> setting, SettingChangeListener<T> listener )
    {
        Entry<T> entry = (Entry<T>) getObserver( setting );
        entry.addListener( listener );
    }

    public <T> void removeListener( Setting<T> setting, SettingChangeListener<T> listener )
    {
        Entry<T> entry = (Entry<T>) getObserver( setting );
        entry.removeListener( listener );
    }

    private class DepEntry<T> extends Entry<T>
    {
        private volatile T solved;
        private DepEntry( SettingImpl<T> setting, T value, T defaultValue, T solved )
        {
            super( setting, value, defaultValue );
            this.solved = solved;
        }

        @Override
        public T getValue()
        {
            return solved;
        }

        @Override
        synchronized void setValue( T value )
        {
            super.setValue( value );
            solved = setting.solveDependency( value != null ? value : defaultValue, getObserver( setting.dependency() ).getValue() );

        }
    }

    private static class Entry<T> implements SettingObserver<T>
    {
        protected final SettingImpl<T> setting;
        protected final T defaultValue;
        private final Collection<SettingChangeListener<T>> updateListeners = new ConcurrentLinkedQueue<>();
        private volatile T value;
        private volatile boolean isDefault;

        private Entry( SettingImpl<T> setting, T value, T defaultValue )
        {
            this.setting = setting;
            this.defaultValue = defaultValue;
            internalSetValue( value );
        }

        @Override
        public T getValue()
        {
            return value;
        }

        synchronized void setValue( T value )
        {
            T oldValue = this.value;
            internalSetValue( value );
            updateListeners.forEach( listener -> listener.accept( oldValue, this.value ) );
        }

        private void internalSetValue( T value )
        {
            isDefault = value == null;
            this.value = isDefault ? defaultValue : value;
        }

        private void addListener( SettingChangeListener<T> listener )
        {
            if ( !setting.dynamic() )
            {
                throw new IllegalArgumentException( "Setting is not dynamic and will not change" );
            }
            updateListeners.add( listener );
        }

        private void removeListener( SettingChangeListener<T> listener )
        {
            updateListeners.remove( listener );
        }

        @Override
        public String toString()
        {
            return setting.valueToString( value ) + (isDefault ? " (default)" : " (configured)");
        }
    }

}

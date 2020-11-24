/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.util.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.commons.lang3.reflect.FieldUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.neo4j.annotations.api.IgnoreApiCheck;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.logging.Log;
import org.neo4j.service.Services;
import org.neo4j.util.Preconditions;

import static java.lang.String.format;
import static java.lang.System.lineSeparator;
import static java.util.Objects.requireNonNull;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.config_command_evaluation_timeout;
import static org.neo4j.configuration.GraphDatabaseSettings.strict_config_validation;

@IgnoreApiCheck
public class Config implements Configuration
{
    public static final String DEFAULT_CONFIG_FILE_NAME = "neo4j.conf";

    public static final class Builder
    {
        private final Collection<Class<? extends SettingsDeclaration>> settingsClasses = new HashSet<>();
        private final Collection<Class<? extends GroupSetting>> groupSettingClasses = new HashSet<>();
        private final Collection<SettingMigrator> settingMigrators = new HashSet<>();
        private final Map<String,String> settingValueStrings = new HashMap<>();
        private final Map<String,Object> settingValueObjects = new HashMap<>();
        private final Map<String,Object> overriddenDefaults = new HashMap<>();
        private final List<Path> configFiles = new ArrayList<>();
        private Config fromConfig;
        private final Log log = new BufferingLog();
        private boolean expandCommands;

        private static boolean allowedToLogOverriddenValues( String setting )
        {
            return !Objects.equals( setting, ExternalSettings.additional_jvm.name() );
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
                fromFile( path, false );
            }
            return this;
        }

        public Builder fromFile( Path cfg )
        {
            return fromFile( cfg, true );
        }

        private Builder fromFile( Path file, boolean allowThrow )
        {
            if ( file == null || Files.notExists( file ) )
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
                if ( Files.isDirectory( file ) )
                {
                    Files.walkFileTree( file, new ConfigDirectoryFileVisitor( file ) );
                }
                else
                {
                    try ( InputStream stream = Files.newInputStream( file ) )
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
                    configFiles.add( file );
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

        public Builder allowCommandExpansion()
        {
            return commandExpansion( true );
        }

        public Builder commandExpansion( boolean expandCommands )
        {
            this.expandCommands = expandCommands;
            return this;
        }

        private Builder()
        {

        }

        public Config build()
        {
            expandCommands |= fromConfig != null && fromConfig.expandCommands; //inherit expandCommands from another config
            if ( expandCommands )
            {
                validateFilePermissionForCommandExpansion( configFiles );
            }
            return new Config( settingsClasses, groupSettingClasses, settingMigrators, settingValueStrings, settingValueObjects, overriddenDefaults,
                    fromConfig, log, expandCommands );
        }

        private void validateFilePermissionForCommandExpansion( List<Path> files )
        {
            if ( files.isEmpty() )
            {
                return;
            }
            String processOwner = SystemUtils.getUserName();
            if ( SystemUtils.IS_OS_UNIX )
            {
                String processGroup = executeCommand( "id -gn", config_command_evaluation_timeout.defaultValue() );

                for ( Path path : files )
                {
                    try
                    {
                        final Set<PosixFilePermission> unixPermission600 = Set.of( PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE );
                        PosixFileAttributes attrs = Files.getFileAttributeView( path, PosixFileAttributeView.class ).readAttributes();
                        Set<PosixFilePermission> permissions = attrs.permissions();
                        if ( !unixPermission600.containsAll( permissions ) ) //actual permission is a subset of required ones
                        {
                            throw new IllegalArgumentException(
                                    format( "%s does not have the correct file permissions to evaluate commands. Has %s, requires at most %s.", path,
                                            permissions, unixPermission600 ) );
                        }

                        String fileOwner = attrs.owner().getName();
                        if ( !fileOwner.equals( processOwner ) )
                        {
                            throw new IllegalArgumentException(
                                    format( "%s does not have the correct file owner to evaluate commands. Has %s, requires %s.", path, fileOwner,
                                            processOwner ) );
                        }

                        String fileGroup = attrs.group().getName();
                        if ( !fileGroup.equals( processGroup ) )
                        {
                            throw new IllegalArgumentException(
                                    format( "%s does not have the correct file group to evaluate commands. Has %s, requires %s.", path, fileGroup,
                                            processGroup ) );
                        }
                    }
                    catch ( IOException | UnsupportedOperationException e )
                    {
                        throw new IllegalStateException( "Unable to access file permissions for " + path, e );
                    }
                }
            }
            else if ( SystemUtils.IS_OS_WINDOWS )
            {
                for ( Path path : files )
                {
                    try
                    {
                        AclFileAttributeView attrs = Files.getFileAttributeView( path, AclFileAttributeView.class );
                        UserPrincipal owner = attrs.getOwner();

                        final Set<AclEntryPermission> windowsUserNoExecute = Set.of( //All but execute for owner
                                AclEntryPermission.READ_DATA, AclEntryPermission.WRITE_DATA, AclEntryPermission.APPEND_DATA,
                                AclEntryPermission.READ_ATTRIBUTES, AclEntryPermission.WRITE_ATTRIBUTES,
                                AclEntryPermission.READ_NAMED_ATTRS, AclEntryPermission.WRITE_NAMED_ATTRS,
                                AclEntryPermission.READ_ACL, AclEntryPermission.WRITE_ACL,
                                AclEntryPermission.DELETE, AclEntryPermission.DELETE_CHILD,
                                AclEntryPermission.WRITE_OWNER, AclEntryPermission.SYNCHRONIZE
                        );
                        for ( AclEntry acl : attrs.getAcl() )
                        {
                            Set<AclEntryPermission> permissions = acl.permissions();
                            if ( AclEntryType.ALLOW.equals( acl.type() ) )
                            {
                                if ( acl.principal().equals( owner ) )
                                {
                                    if ( !windowsUserNoExecute.containsAll( permissions ) )
                                    {
                                        throw new IllegalArgumentException(
                                                format( "%s does not have the correct ACL for owner to evaluate commands. Has %s for %s, requires at most %s.",
                                                        path, permissions, acl.principal().getName(), windowsUserNoExecute ) );
                                    }
                                }
                                else
                                {
                                    if ( !permissions.isEmpty() )
                                    {
                                        throw new IllegalArgumentException(
                                                format( "%s does not have the correct ACL. Has %s for %s, should be none for all except owner.",
                                                        path, permissions, acl.principal().getName() ) );
                                    }
                                }
                            }
                        }

                        String domainAndName = owner.getName();
                        String fileOwner = domainAndName.contains( "\\" ) ? domainAndName.split( "\\\\" )[1] : domainAndName; //remove domain
                        if ( !fileOwner.equals( processOwner ) )
                        {
                            throw new IllegalArgumentException(
                                    format( "%s does not have the correct file owner to evaluate commands. Has %s, requires %s.", path, domainAndName,
                                            processOwner ) );
                        }
                    }
                    catch ( IOException | UnsupportedOperationException e )
                    {
                        throw new IllegalStateException( "Unable to access file permissions for " + path, e );
                    }
                }
            }
            else
            {
                throw new IllegalStateException( "Configuration command expansion not supported for " + SystemUtils.OS_NAME );
            }
        }

        private class ConfigDirectoryFileVisitor implements FileVisitor<Path>
        {
            private final Path root;

            ConfigDirectoryFileVisitor( Path root )
            {
                this.root = root;
            }

            private boolean isRoot( Path dir )
            {
                return root.equals( dir );
            }

            private boolean isNotHidden( Path file )
            {
                return !file.getFileName().toString().startsWith( "." );
            }

            private boolean isFile( Path file, BasicFileAttributes attrs )
            {
                return attrs.isRegularFile() || Files.isRegularFile( file );
            }

            @Override
            public FileVisitResult preVisitDirectory( Path dir, BasicFileAttributes attrs )
            {
                if ( isRoot( dir ) )
                {
                    return FileVisitResult.CONTINUE;
                }
                else
                {
                    // We don't go into subdirectories, it's too risky
                    if ( isNotHidden( dir ) )
                    {
                        log.warn( "Ignoring subdirectory in config directory [" + dir + "]." );
                    }
                    return FileVisitResult.SKIP_SUBTREE;
                }
            }

            @Override
            public FileVisitResult visitFile( Path file, BasicFileAttributes attrs ) throws IOException
            {
                if ( isNotHidden( file ) && isFile( file, attrs ) )
                {
                    String key = file.getFileName().toString();
                    String value = Files.readString( file );
                    setRaw( key, value );
                    configFiles.add( file );
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed( Path file, IOException exc ) throws IOException
            {
                throw exc != null ? exc : new IOException( "Unknown failure loading config file [" + file.toAbsolutePath() + "]" );
            }

            @Override
            public FileVisitResult postVisitDirectory( Path dir, IOException exc ) throws IOException
            {
                if ( exc != null )
                {
                    throw exc;
                }
                return FileVisitResult.CONTINUE;
            }
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
    private final boolean expandCommands;
    private final Configuration validationConfig = new ValidationConfig();
    private Duration commandEvaluationTimeout = config_command_evaluation_timeout.defaultValue();

    protected Config()
    {
        expandCommands = false;
    }

    private Config( Collection<Class<? extends SettingsDeclaration>> settingsClasses,
            Collection<Class<? extends GroupSetting>> groupSettingClasses,
            Collection<SettingMigrator> settingMigrators,
            Map<String,String> settingValueStrings,
            Map<String,Object> settingValueObjects,
            Map<String,Object> overriddenDefaultObjects,
            Config fromConfig,
            Log log,
            boolean expandCommands )
    {
        this.log = log;
        this.expandCommands = expandCommands;

        if ( expandCommands )
        {
            log.info( "Command expansion is explicitly enabled for configuration" );
        }

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
        Set<String> keys = new HashSet<>( definedSettings.keySet() );
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

        if ( keys.remove( config_command_evaluation_timeout.name() ) )
        {
            evaluateSetting( config_command_evaluation_timeout, settingValueStrings, settingValueObjects,
                    fromConfig, overriddenDefaultStrings, overriddenDefaultObjects );
            commandEvaluationTimeout = get( config_command_evaluation_timeout );
        }

        newSettings.addAll( getActiveSettings( keys, definedGroups, definedSettings, strict ) );

        evaluateSettingValues( newSettings, settingValueStrings, settingValueObjects, overriddenDefaultStrings, overriddenDefaultObjects, fromConfig );
    }

    @SuppressWarnings( "unchecked" )
    private void evaluateSettingValues( Collection<SettingImpl<?>> settingsToEvaluate, Map<String,String> settingValueStrings,
            Map<String,Object> settingValueObjects,Map<String,String> overriddenDefaultStrings, Map<String,Object> overriddenDefaultObjects, Config fromConfig )
    {
        Deque<SettingImpl<?>> newSettings = new ArrayDeque<>( settingsToEvaluate );
        while ( !newSettings.isEmpty() )
        {
            boolean modified = false;
            SettingImpl<?> last = newSettings.peekLast();
            SettingImpl<Object> setting;
            Map<Setting<?>,Setting<?>> dependencies = new HashMap<>();
            do
            {
                setting = (SettingImpl<Object>) requireNonNull( newSettings.pollFirst() );

                boolean retry = false;
                if ( setting.dependency() != null && !settings.containsKey( setting.dependency().name() ) )
                {
                    //dependency not yet evaluated
                    dependencies.put( setting, setting.dependency() );
                    retry = true;
                }
                else
                {
                    try
                    {
                        evaluateSetting( setting, settingValueStrings, settingValueObjects, fromConfig, overriddenDefaultStrings, overriddenDefaultObjects );
                        modified = true;
                    }
                    catch ( AccessDuringEvaluationException e )
                    {
                        //Constraint with internal dependencies yet not evaluated
                        dependencies.put( setting, e.getAttemptedAccess() );
                        retry = true;
                    }
                }
                if ( retry )
                {
                    newSettings.addLast( setting );
                }
            }
            while ( setting != last );

            if ( !modified && !newSettings.isEmpty() )
            {
                //Settings left depend on settings not present in this config.
                String unsolvable = newSettings.stream()
                        .map( s -> format("'%s'->'%s'", s.name(), dependencies.get( s ).name() ) )
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
                defaultValue = setting.parse( evaluateIfCommand( key, overriddenDefaultStrings.get( key ) ) );
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
                value = setting.parse( evaluateIfCommand( key, settingValueStrings.get( key ) ) );
            }
            else if ( fromConfig != null && fromConfig.settings.containsKey( key ) )
            {
                Entry<?> entry = fromConfig.settings.get( key );
                value = entry.isDefault ? null : entry.value;
            }

            value = setting.solveDefault( value, defaultValue );

            settings.put( key, createEntry( setting, value, defaultValue ) );
        }
        catch ( AccessDuringEvaluationException exception )
        {
            throw exception; //Bubble up
        }
        catch ( RuntimeException exception )
        {
            String msg = format( "Error evaluating value for setting '%s'. %s", setting.name(), exception.getMessage() );
            throw new IllegalArgumentException( msg, exception );
        }
    }

    private String evaluateIfCommand( String settingName, String entry )
    {
        if ( isCommand( entry ) )
        {
            Preconditions.checkArgument( expandCommands, format( "%s is a command, but config is not explicitly told to expand it.", entry )  );
            String str = entry.trim();
            String command = str.substring( 2, str.length() - 1 );
            log.info( "Executing external script to retrieve value of setting " + settingName );
            return executeCommand( command, commandEvaluationTimeout );
        }
        return entry;
    }

    private static boolean isCommand( String entry )
    {
        String str = entry.trim();
        return str.length() > 3 && str.charAt( 0 ) == '$' && str.charAt( 1 ) == '(' && str.charAt( str.length() - 1 ) == ')';
    }

    private static String executeCommand( String command, Duration timeout )
    {
        Process process = null;
        try
        {
            String[] commands = CommandLine.parse( command ).toStrings();
            //Unquote the arguments, as ProcessBuilder does not handle that
            for ( int i = 1; i < commands.length; i++ )
            {
                String arg = commands[i];
                if ( StringUtils.isQuoted( arg ) )
                {
                    commands[i] = arg.substring( 1, arg.length() - 1 );
                }
            }
            process = new ProcessBuilder( commands ).start();
            BufferedReader out = new BufferedReader( new InputStreamReader( process.getInputStream() ) );
            BufferedReader err = new BufferedReader( new InputStreamReader( process.getErrorStream() ) );
            if ( !process.waitFor( timeout.toMillis(), TimeUnit.MILLISECONDS ) )
            {
                throw new IllegalArgumentException( format( "Timed out executing command `%s`", command ) );
            }
            String output = out.lines().collect( Collectors.joining( lineSeparator() ) );

            int exitCode = process.exitValue();
            if ( exitCode != 0 )
            {
                String errOutput =  err.lines().collect( Collectors.joining( lineSeparator() ) );
                throw new IllegalArgumentException( format( "Command `%s` failed with exit code %s.%n%s%n%s", command, exitCode, output, errOutput ) );
            }
            return output;
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            throw new IllegalArgumentException( "Interrupted while executing command", e );
        }
        catch ( IOException e )
        {
            throw new IllegalArgumentException( e );
        }
        finally
        {
            if ( process != null && process.isAlive() )
            {
                process.destroyForcibly();
            }
        }
    }

    @SuppressWarnings( "unchecked" )
    private <T> Entry<T> createEntry( SettingImpl<T> setting, T value, T defaultValue )
    {
        if ( setting.dependency() != null )
        {
            var dep = settings.get( setting.dependency().name() );
            T solvedValue = setting.solveDependency( value != null ? value : defaultValue, (T) dep.getValue() );
            return new DepEntry<>( setting, value, defaultValue, solvedValue );
        }
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
        Map<Setting<Object>,Object> values = new HashMap<>();
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
            super( setting, value, defaultValue, false );
            this.solved = solved;
            setting.validate( solved, validationConfig );
        }

        @Override
        public T getValue()
        {
            return solved;
        }

        @Override
        synchronized void setValue( T value )
        {
            T oldValue = solved;
            solved = setting.solveDependency( value != null ? value : defaultValue, getObserver( setting.dependency() ).getValue() );
            setting.validate( solved, validationConfig );
            internalSetValue( value );
            notifyListeners( oldValue, solved );
        }
    }

    private class Entry<T> implements SettingObserver<T>
    {
        protected final SettingImpl<T> setting;
        protected final T defaultValue;
        private final boolean validate;
        private final Collection<SettingChangeListener<T>> updateListeners = new ConcurrentLinkedQueue<>();
        private volatile T value;
        private volatile boolean isDefault;

        private Entry( SettingImpl<T> setting, T value, T defaultValue )
        {
            this( setting, value, defaultValue, true );
        }

        private Entry( SettingImpl<T> setting, T value, T defaultValue, boolean validate )
        {
            this.setting = setting;
            this.defaultValue = defaultValue;
            this.validate = validate;
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
            notifyListeners( oldValue, this.value );
        }

        void internalSetValue( T value )
        {
            isDefault = value == null;
            this.value = isDefault ? defaultValue : value;
            if ( validate )
            {
                setting.validate( this.value, validationConfig );
            }
        }

        protected void notifyListeners( T oldValue, T newValue )
        {
            updateListeners.forEach( listener -> listener.accept( oldValue, newValue ) );
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

    private static class AccessDuringEvaluationException extends RuntimeException
    {
        private final Setting<?> attemptedAccess;

        AccessDuringEvaluationException( Setting<?> attemptedAccess )
        {
            super( format( "AccessDuringEvaluationException{ Tried to access %s in config during construction }", attemptedAccess.name() ) );
            this.attemptedAccess = attemptedAccess;
        }

        Setting<?> getAttemptedAccess()
        {
            return attemptedAccess;
        }
    }

    private class ValidationConfig implements Configuration
    {
        @Override
        public <T> T get( Setting<T> setting )
        {
            if ( setting.dynamic() )
            {
                throw new IllegalArgumentException( "Can not depend on dynamic setting:" + setting.name() );
            }
            if ( !settings.containsKey( setting.name() ) )
            {
                throw new AccessDuringEvaluationException( setting );
            }
            return Config.this.get( setting );
        }
    }
}

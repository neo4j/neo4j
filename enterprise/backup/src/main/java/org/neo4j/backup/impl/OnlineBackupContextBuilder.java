/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.backup.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.arguments.Arguments;
import org.neo4j.commandline.arguments.MandatoryNamedArg;
import org.neo4j.commandline.arguments.OptionalBooleanArg;
import org.neo4j.commandline.arguments.OptionalNamedArg;
import org.neo4j.commandline.arguments.common.MandatoryCanonicalPath;
import org.neo4j.commandline.arguments.common.OptionalCanonicalPath;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.TimeUtil;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.util.OptionalHostnamePort;

import static org.neo4j.consistency.ConsistencyCheckSettings.consistency_check_graph;
import static org.neo4j.consistency.ConsistencyCheckSettings.consistency_check_indexes;
import static org.neo4j.consistency.ConsistencyCheckSettings.consistency_check_label_scan_store;
import static org.neo4j.consistency.ConsistencyCheckSettings.consistency_check_property_owners;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.logical_logs_location;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.kernel.impl.util.Converters.toOptionalHostnamePortFromRawAddress;

class OnlineBackupContextBuilder
{
    static final String ARG_NAME_BACKUP_DIRECTORY = "backup-dir";
    static final String ARG_DESC_BACKUP_DIRECTORY = "Directory to place backup in.";

    static final String ARG_NAME_BACKUP_NAME = "name";
    static final String ARG_DESC_BACKUP_NAME =
            "Name of backup. If a backup with this name already exists an incremental backup will be attempted.";

    static final String ARG_NAME_BACKUP_SOURCE = "from";
    static final String ARG_DESC_BACKUP_SOURCE = "Host and port of Neo4j.";
    static final String ARG_DFLT_BACKUP_SOURCE = "localhost:6362";

    static final String ARG_NAME_PROTO_OVERRIDE = "protocol";
    static final String ARG_DESC_PROTO_OVERRIDE = "Preferred backup protocol";
    static final String ARG_DFLT_PROTO_OVERRIDE = "any";

    static final String ARG_NAME_TIMEOUT = "timeout";
    static final String ARG_DESC_TIMEOUT =
            "Timeout in the form <time>[ms|s|m|h], where the default unit is seconds.";
    static final String ARG_DFLT_TIMEOUT = "20m";

    static final String ARG_NAME_PAGECACHE = "pagecache";
    static final String ARG_DESC_PAGECACHE = "The size of the page cache to use for the backup process.";
    static final String ARG_DFLT_PAGECACHE = "8m";

    static final String ARG_NAME_REPORT_DIRECTORY = "cc-report-dir";
    static final String ARG_DESC_REPORT_DIRECTORY = "Directory where consistency report will be written.";

    static final String ARG_NAME_ADDITIONAL_CONFIG_DIR = "additional-config";
    static final String ARG_DESC_ADDITIONAL_CONFIG_DIR =
            "Configuration file to supply additional configuration in. This argument is DEPRECATED.";

    static final String ARG_NAME_FALLBACK_FULL = "fallback-to-full";
    static final String ARG_DESC_FALLBACK_FULL =
            "If an incremental backup fails backup will move the old backup to <name>.err.<N> and fallback to a full " +
            "backup instead.";

    static final String ARG_NAME_CHECK_CONSISTENCY = "check-consistency";
    static final String ARG_DESC_CHECK_CONSISTENCY = "If a consistency check should be made.";

    static final String ARG_NAME_CHECK_GRAPH = "cc-graph";
    static final String ARG_DESC_CHECK_GRAPH =
            "Perform consistency checks between nodes, relationships, properties, types and tokens.";

    static final String ARG_NAME_CHECK_INDEXES = "cc-indexes";
    static final String ARG_DESC_CHECK_INDEXES = "Perform consistency checks on indexes.";

    static final String ARG_NAME_CHECK_LABELS = "cc-label-scan-store";
    static final String ARG_DESC_CHECK_LABELS = "Perform consistency checks on the label scan store.";

    static final String ARG_NAME_CHECK_OWNERS = "cc-property-owners";
    static final String ARG_DESC_CHECK_OWNERS =
            "Perform additional consistency checks on property ownership. This check is *very* expensive in time and " +
            "memory.";

    private final Path homeDir;
    private final Path configDir;

    OnlineBackupContextBuilder( Path homeDir, Path configDir )
    {
        this.homeDir = homeDir;
        this.configDir = configDir;
    }

    public static Arguments arguments()
    {
        String argExampleProtoOverride = Stream.of( SelectedBackupProtocol.values() )
                .map( SelectedBackupProtocol::getName )
                .sorted()
                .collect( Collectors.joining( "|" ) );
        return new Arguments()
                .withArgument( new MandatoryCanonicalPath(
                        ARG_NAME_BACKUP_DIRECTORY, "backup-path", ARG_DESC_BACKUP_DIRECTORY ) )
                .withArgument( new MandatoryNamedArg(
                        ARG_NAME_BACKUP_NAME, "graph.db-backup", ARG_DESC_BACKUP_NAME ) )
                .withArgument( new OptionalNamedArg(
                        ARG_NAME_BACKUP_SOURCE, "address", ARG_DFLT_BACKUP_SOURCE, ARG_DESC_BACKUP_SOURCE ) )
                .withArgument( new OptionalNamedArg( ARG_NAME_PROTO_OVERRIDE, argExampleProtoOverride,
                        ARG_DFLT_PROTO_OVERRIDE, ARG_DESC_PROTO_OVERRIDE ) )
                .withArgument( new OptionalBooleanArg(
                        ARG_NAME_FALLBACK_FULL, true, ARG_DESC_FALLBACK_FULL ) )
                .withArgument( new OptionalNamedArg(
                        ARG_NAME_TIMEOUT, "timeout", ARG_DFLT_TIMEOUT, ARG_DESC_TIMEOUT ) )
                .withArgument( new OptionalNamedArg(
                        ARG_NAME_PAGECACHE, "8m", ARG_DFLT_PAGECACHE, ARG_DESC_PAGECACHE ) )
                .withArgument( new OptionalBooleanArg(
                        ARG_NAME_CHECK_CONSISTENCY, true, ARG_DESC_CHECK_CONSISTENCY ) )
                .withArgument( new OptionalCanonicalPath(
                        ARG_NAME_REPORT_DIRECTORY, "directory", ".", ARG_DESC_REPORT_DIRECTORY ) )
                .withArgument( new OptionalCanonicalPath(
                        ARG_NAME_ADDITIONAL_CONFIG_DIR, "config-file-path", "", ARG_DESC_ADDITIONAL_CONFIG_DIR ) )
                .withArgument( new OptionalBooleanArg(
                        ARG_NAME_CHECK_GRAPH, true, ARG_DESC_CHECK_GRAPH ) )
                .withArgument( new OptionalBooleanArg(
                        ARG_NAME_CHECK_INDEXES, true, ARG_DESC_CHECK_INDEXES ) )
                .withArgument( new OptionalBooleanArg(
                        ARG_NAME_CHECK_LABELS, true, ARG_DESC_CHECK_LABELS ) )
                .withArgument( new OptionalBooleanArg(
                        ARG_NAME_CHECK_OWNERS, false, ARG_DESC_CHECK_OWNERS ) );
    }

    public OnlineBackupContext createContext( String... args ) throws IncorrectUsage, CommandFailed
    {
        try
        {
            Arguments arguments = arguments();
            arguments.parse( args );

            OptionalHostnamePort address = toOptionalHostnamePortFromRawAddress(
                    arguments.get( ARG_NAME_BACKUP_SOURCE ) );
            Path folder = getBackupDirectory( arguments );
            String name = arguments.get( ARG_NAME_BACKUP_NAME );
            boolean fallbackToFull = arguments.getBoolean( ARG_NAME_FALLBACK_FULL );
            boolean doConsistencyCheck = arguments.getBoolean( ARG_NAME_CHECK_CONSISTENCY );
            long timeout = arguments.get( ARG_NAME_TIMEOUT, TimeUtil.parseTimeMillis );
            SelectedBackupProtocol selectedBackupProtocol = SelectedBackupProtocol.fromUserInput( arguments.get( ARG_NAME_PROTO_OVERRIDE ) );
            String pagecacheMemory = arguments.get( ARG_NAME_PAGECACHE );
            Optional<Path> additionalConfig = arguments.getOptionalPath( ARG_NAME_ADDITIONAL_CONFIG_DIR );
            Path reportDir = arguments.getOptionalPath( ARG_NAME_REPORT_DIRECTORY ).orElseThrow(
                    () -> new IllegalArgumentException( ARG_NAME_REPORT_DIRECTORY + " must be a path" ) );

            OnlineBackupRequiredArguments requiredArguments = new OnlineBackupRequiredArguments(
                    address, folder, name, selectedBackupProtocol, fallbackToFull, doConsistencyCheck, timeout, reportDir );

            Path configFile = configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME );
            Config.Builder builder = Config.fromFile( configFile );
            Path logPath = requiredArguments.getResolvedLocationFromName();
            Config config = builder.withHome( homeDir )
                                   .withSetting( logical_logs_location, logPath.toString() )
                                   .withConnectorsDisabled()
                                   .build();
            additionalConfig.map( this::loadAdditionalConfigFile ).ifPresent( config::augment );
            // We only replace the page cache memory setting.
            // Any other custom page swapper, etc. settings are preserved and used.
            config.augment( pagecache_memory, pagecacheMemory );

            // Disable prometheus to avoid binding exceptions
            config.augment( "metrics.prometheus.enabled", Settings.FALSE );

            // Build consistency-checker configuration.
            // Note: We can remove the loading from config file in 4.0.
            BiFunction<String,Setting<Boolean>,Boolean> oneOf =
                    ( a, s ) -> arguments.has( a ) ? arguments.getBoolean( a ) : config.get( s );
            ConsistencyFlags consistencyFlags = new ConsistencyFlags(
                    oneOf.apply( ARG_NAME_CHECK_GRAPH, consistency_check_graph ),
                    oneOf.apply( ARG_NAME_CHECK_INDEXES, consistency_check_indexes ),
                    oneOf.apply( ARG_NAME_CHECK_LABELS, consistency_check_label_scan_store ),
                    oneOf.apply( ARG_NAME_CHECK_OWNERS, consistency_check_property_owners ) );
            return new OnlineBackupContext( requiredArguments, config, consistencyFlags );
        }
        catch ( IllegalArgumentException e )
        {
            throw new IncorrectUsage( e.getMessage() );
        }
        catch ( UncheckedIOException e )
        {
            throw new CommandFailed( e.getMessage(), e );
        }
    }

    private Path getBackupDirectory( Arguments arguments ) throws CommandFailed
    {
        Path path = arguments.getMandatoryPath( ARG_NAME_BACKUP_DIRECTORY );
        try
        {
            return path.toRealPath();
        }
        catch ( IOException e )
        {
            throw new CommandFailed( String.format( "Directory '%s' does not exist.", path ) );
        }
    }

    private Config loadAdditionalConfigFile( Path path )
    {
        try ( InputStream in = Files.newInputStream( path ) )
        {
            return Config.fromSettings( MapUtil.load( in ) ).build();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException(
                    "Could not read additional configuration from " + path + ". " +
                    "The file either does not exist, is not a regular file, or is not readable.", e );
        }
    }
}

/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.backup;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.arguments.Arguments;
import org.neo4j.commandline.arguments.MandatoryNamedArg;
import org.neo4j.commandline.arguments.OptionalBooleanArg;
import org.neo4j.commandline.arguments.OptionalNamedArg;
import org.neo4j.commandline.arguments.common.OptionalCanonicalPath;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.ConsistencyCheckSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.configuration.ConfigLoader;

import static java.util.Arrays.asList;
import static org.neo4j.kernel.impl.util.Converters.toHostnamePort;

public class OnlineBackupCommand implements AdminCommand
{

    public static final Arguments arguments = new Arguments()
            .withArgument( new OptionalNamedArg( "from", "address", "localhost:6362",
                    "Host and port of Neo4j." ) )
            .withArgument( new MandatoryNamedArg( "to", "backup-path",
                    "Directory where the backup will be made; if there is already a backup present an " +
                            "incremental backup will be attempted." ) )
            .withArgument( new OptionalBooleanArg( "check-consistency", true,
                    "If a consistency check should be made." ) )
            .withArgument( new OptionalCanonicalPath( "cc-report-dir", "directory", ".",
                    "Directory where consistency report will be written.") )
            .withAdditionalConfig()
            .withArgument( new OptionalNamedArg( "timeout", "timeout", "20m",
                    "Timeout in the form <time>[ms|s|m|h], where the default unit is seconds." ) );

    public static class Provider extends AdminCommand.Provider
    {
        public Provider()
        {
            super( "backup" );
        }

        @Override
        public Arguments allArguments()
        {
            return arguments;
        }

        @Override
        public String description()
        {
            return "Perform a backup, over the network, from a running Neo4j server into a local copy " +
                    "of the database store (the backup). Neo4j Server must be configured to run a backup service. " +
                    "See http://neo4j.com/docs/operations-manual/current/backup/ for more details.\n" +
                    "\n" +
                    "WARNING: this command is experimental and subject to change.";
        }

        @Override
        public String summary()
        {
            return "Perform a backup, over the network, from a running Neo4j server.";
        }

        @Override
        public AdminCommand create( Path homeDir, Path configDir, OutsideWorld outsideWorld )
        {
            return new OnlineBackupCommand(
                    new BackupTool( new BackupService( outsideWorld.errorStream() ), outsideWorld.errorStream() ),
                    homeDir, configDir, new ConsistencyCheckService() );
        }
    }

    private final BackupTool backupTool;
    private final Path homeDir;
    private final Path configDir;
    private ConsistencyCheckService consistencyCheckService;

    public OnlineBackupCommand( BackupTool backupTool, Path homeDir, Path configDir,
                                ConsistencyCheckService consistencyCheckService )
    {
        this.backupTool = backupTool;
        this.homeDir = homeDir;
        this.configDir = configDir;
        this.consistencyCheckService = consistencyCheckService;
    }

    @Override
    public void execute( String[] args ) throws IncorrectUsage, CommandFailed
    {
        HostnamePort address;
        File destination;
        ConsistencyCheck consistencyCheck = ConsistencyCheck.NONE;
        Optional<Path> additionalConfig;
        long timeout;
        try
        {
            address = toHostnamePort( new HostnamePort( "localhost", 6362 ) )
                    .apply( arguments.parse( "from", args ) );
            destination = arguments.parseMandatoryPath( "to", args ).toFile();
            timeout = parseTimeout( args );
            additionalConfig = arguments.parseOptionalPath( "additional-config", args );
            if ( arguments.parseBoolean( "check-consistency", args ) )
            {
                Path reportDir = arguments.parseOptionalPath( "cc-report-dir", args )
                        .orElseThrow( () ->
                        new IllegalArgumentException( "cc-report-dir must be a path" ) );
                consistencyCheck = ConsistencyCheck.full( reportDir.toFile(), consistencyCheckService );
            }
        }
        catch ( IllegalArgumentException e )
        {
            throw new IncorrectUsage( e.getMessage() );
        }

        try
        {
            backupTool.executeBackup(
                    address, destination, consistencyCheck, loadConfig( additionalConfig ), timeout, false );
        }
        catch ( BackupTool.ToolFailureException e )
        {
            throw new CommandFailed( "backup failed: " + e.getMessage(), e );
        }
    }

    private long parseTimeout( String[] args )
    {
        return Args.parse( args ).getDuration( "timeout", TimeUnit.MINUTES.toMillis( 20 ) );
    }

    private Config loadConfig( Optional<Path> additionalConfig ) throws CommandFailed
    {
        //noinspection unchecked
        return withAdditionalConfig( additionalConfig,
                new ConfigLoader( asList( GraphDatabaseSettings.class, ConsistencyCheckSettings.class ) )
                        .loadOfflineConfig(
                                Optional.of( homeDir.toFile() ),
                                Optional.of( configDir.resolve( "neo4j.conf" ).toFile() ) ) );
    }

    private Config withAdditionalConfig( Optional<Path> additionalConfig, Config config ) throws CommandFailed
    {
        if ( additionalConfig.isPresent() )
        {
            try
            {
                return config.with( MapUtil.load( additionalConfig.get().toFile() ) );
            }
            catch ( IOException e )
            {
                throw new CommandFailed( "Could not read additional config from " + additionalConfig.get(), e );
            }
        }
        return config;
    }
}

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
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.commandline.arguments.Arguments;
import org.neo4j.commandline.arguments.MandatoryNamedArg;
import org.neo4j.commandline.arguments.OptionalNamedArg;
import org.neo4j.consistency.ConsistencyCheckSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.configuration.ConfigLoader;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import static org.neo4j.kernel.impl.util.Converters.mandatory;
import static org.neo4j.kernel.impl.util.Converters.optional;
import static org.neo4j.kernel.impl.util.Converters.toFile;
import static org.neo4j.kernel.impl.util.Converters.toHostnamePort;
import static org.neo4j.kernel.impl.util.Converters.toPath;
import static org.neo4j.kernel.impl.util.Converters.withDefault;

public class OnlineBackupCommand implements AdminCommand
{

    public static final Arguments arguments = new Arguments()
            .withArgument( new OptionalNamedArg( "from", "address", "localhost:6362",
                    "Host and port of Neo4j." ) )
            .withArgument( new MandatoryNamedArg( "to", "backup-path",
                    "Directory where the backup will be made; if there is already a backup present an " +
                            "incremental backup will be attempted." ) )
            .withArgument( new OptionalNamedArg( "check-consistency", "true|false", "true", "If a consistency" +
                    " check should be made." ) )
            .withAdditionalConfig()
            .withArgument( new OptionalNamedArg( "timeout", "timeout", "20m",
                    "Timeout in the form <time>[ms|s|m|h], where the default unit is seconds." ) );
    private static final String checkConsistencyArg = "check-consistency";

    public static class Provider extends AdminCommand.Provider
    {
        public Provider()
        {
            super( "backup" );
        }

        @Override
        public Arguments arguments()
        {
            return arguments;
        }

        @Override
        public String description()
        {
            return "Perform a backup, over the network, from a running Neo4j server into a local copy of the " +
                            "database store (the backup). Neo4j Server must be configured to run a backup service. " +
                            "See http://neo4j.com/docs/operations-manual/current/backup/ for more details.";
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
        Args parsedArgs = Args.withFlags( checkConsistencyArg ).parse( args );
        HostnamePort address;
        File destination;
        ConsistencyCheck consistencyCheck;
        Optional<Path> additionalConfig;
        long timeout;
        try
        {
            address = parseAddress( parsedArgs );
            destination = parseDestination( parsedArgs );
            consistencyCheck = parseConsistencyCheck( parsedArgs );
            additionalConfig = parseAdditionalConfig( parsedArgs );
            timeout = parseTimeout( parsedArgs );
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

    private HostnamePort parseAddress( Args args )
    {
        HostnamePort defaultAddress = new HostnamePort( "localhost", 6362 );
        return args.interpretOption( "from", withDefault( defaultAddress ), toHostnamePort( defaultAddress ) );
    }

    private File parseDestination( Args parsedArgs )
    {
        return parsedArgs.interpretOption( "to", mandatory(), toFile() );
    }

    private ConsistencyCheck parseConsistencyCheck( Args args ) throws CommandFailed
    {
        File reportDir;
        try
        {
            reportDir = args.interpretOption( "cc-report-dir", withDefault( new File(".") ), File::new )
                    .getCanonicalFile();
        }
        catch ( IOException e )
        {
            throw new CommandFailed( format( "cannot access report directory: %s: %s",
                    e.getClass().getSimpleName(), e.getMessage() ), e );
        }
        return args.getBoolean( checkConsistencyArg, true, true ) ?
                ConsistencyCheck.full( reportDir, consistencyCheckService ) : ConsistencyCheck.NONE;
    }

    private Optional<Path> parseAdditionalConfig( Args args )
    {
        return Optional.ofNullable( args.interpretOption( "additional-config", optional(), toPath() ) );
    }

    private long parseTimeout( Args parsedArgs )
    {
        return parsedArgs.getDuration( "timeout", TimeUnit.MINUTES.toMillis( 20 ) );
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

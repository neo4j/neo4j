/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import org.neo4j.commandline.arguments.common.MandatoryCanonicalPath;
import org.neo4j.commandline.arguments.common.OptionalCanonicalPath;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.ConsistencyCheckSettings;
import org.neo4j.consistency.checking.full.CheckConsistencyConfig;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.TimeUtil;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.server.configuration.ConfigLoader;

import static org.neo4j.kernel.impl.util.Converters.toHostnamePort;

public class OnlineBackupCommand implements AdminCommand
{

    private static final Arguments arguments = new Arguments()
            .withArgument( new MandatoryCanonicalPath( "backup-dir", "backup-path",
                    "Directory to place backup in." ) )
            .withArgument( new MandatoryNamedArg( "name", "graph.db-backup",
                    "Name of backup. If a backup with this name already exists an incremental backup will be " +
                            "attempted." ) )
            .withArgument( new OptionalNamedArg( "from", "address", "localhost:6362",
                    "Host and port of Neo4j." ) )
            .withArgument( new OptionalBooleanArg( "fallback-to-full", true,
                    "If an incremental backup fails backup will move the old backup to <name>.err.<N> and " +
                            "fallback to a full backup instead." ) )
            .withArgument( new OptionalNamedArg( "timeout", "timeout", "20m",
                    "Timeout in the form <time>[ms|s|m|h], where the default unit is seconds." ) )
            .withArgument( new OptionalBooleanArg( "check-consistency", true,
                    "If a consistency check should be made." ) )
            .withArgument( new OptionalCanonicalPath( "cc-report-dir", "directory", ".",
                    "Directory where consistency report will be written." ) )
            .withArgument( new OptionalCanonicalPath( "additional-config", "config-file-path", "",
                    "Configuration file to supply additional configuration in. This argument is DEPRECATED." ) )
            .withArgument( new OptionalBooleanArg( "cc-graph", true,
                    "Perform consistency checks between nodes, relationships, properties, types and tokens." ) )
            .withArgument( new OptionalBooleanArg( "cc-indexes", true,
                    "Perform consistency checks on indexes." ) )
            .withArgument( new OptionalBooleanArg( "cc-label-scan-store", true,
                    "Perform consistency checks on the label scan store." ) )
            .withArgument( new OptionalBooleanArg( "cc-property-owners", false,
                    "Perform additional consistency checks on property ownership. This check is *very* expensive in " +
                            "time and memory." ) );

    public static Arguments arguments()
    {
        return arguments;
    }

    static final int STATUS_CC_ERROR = 2;
    static final int STATUS_CC_INCONSISTENT = 3;
    static final int MAX_OLD_BACKUPS = 1000;
    private final BackupService backupService;
    private final Path homeDir;
    private final Path configDir;
    private ConsistencyCheckService consistencyCheckService;
    private final OutsideWorld outsideWorld;

    public OnlineBackupCommand( BackupService backupService, Path homeDir, Path configDir,
            ConsistencyCheckService consistencyCheckService,
            OutsideWorld outsideWorld )
    {
        this.backupService = backupService;
        this.homeDir = homeDir;
        this.configDir = configDir;
        this.consistencyCheckService = consistencyCheckService;
        this.outsideWorld = outsideWorld;
    }

    @Override
    public void execute( String[] args ) throws IncorrectUsage, CommandFailed
    {
        final HostnamePort address;
        final Path folder;
        final String name;
        final boolean fallbackToFull;
        final boolean doConsistencyCheck;
        final Optional<Path> additionalConfig;
        final Path reportDir;
        final long timeout;
        final boolean checkGraph;
        final boolean checkIndexes;
        final boolean checkLabelScanStore;
        final boolean checkPropertyOwners;

        try
        {
            address = toHostnamePort( new HostnamePort( "localhost", 6362 ) )
                    .apply( arguments.parse( args ).get( "from" ) );
            folder = arguments.getMandatoryPath( "backup-dir" );
            name = arguments.get( "name" );
            fallbackToFull = arguments.getBoolean( "fallback-to-full" );
            doConsistencyCheck = arguments.getBoolean( "check-consistency" );
            timeout = arguments.get( "timeout", TimeUtil.parseTimeMillis );
            additionalConfig = arguments.getOptionalPath( "additional-config" );
            reportDir = arguments.getOptionalPath( "cc-report-dir" ).orElseThrow( () ->
                    new IllegalArgumentException( "cc-report-dir must be a path" ) );
        }
        catch ( IllegalArgumentException e )
        {
            throw new IncorrectUsage( e.getMessage() );
        }

        // Make sure destination exists
        if ( !outsideWorld.fileSystem().isDirectory( folder.toFile() ) )
        {
            throw new CommandFailed( String.format( "Directory '%s' does not exist.", folder ) );
        }

        if ( !outsideWorld.fileSystem().isDirectory( reportDir.toFile() ) )
        {
            throw new CommandFailed( String.format( "Directory '%s' does not exist.", reportDir ) );
        }

        File destination = folder.resolve( name ).toFile();
        Config config = loadConfig( additionalConfig );
        boolean done = false;

        try
        {
            // We can remove the loading from config file in 4.0
            if ( arguments.has( "cc-graph" ) )
            {
                checkGraph = arguments.getBoolean( "cc-graph" );
            }
            else
            {
                checkGraph = ConsistencyCheckSettings.consistency_check_graph.from( config );
            }
            if ( arguments.has( "cc-indexes" ) )
            {
                checkIndexes = arguments.getBoolean( "cc-indexes" );
            }
            else
            {
                checkIndexes = ConsistencyCheckSettings.consistency_check_indexes.from( config );
            }
            if ( arguments.has( "cc-label-scan-store" ) )
            {
                checkLabelScanStore = arguments.getBoolean( "cc-label-scan-store" );
            }
            else
            {
                checkLabelScanStore = ConsistencyCheckSettings.consistency_check_label_scan_store.from( config );
            }
            if ( arguments.has( "cc-property-owners" ) )
            {
                checkPropertyOwners = arguments.getBoolean( "cc-property-owners" );
            }
            else
            {
                checkPropertyOwners = ConsistencyCheckSettings.consistency_check_property_owners.from( config );
            }
        }
        catch ( IllegalArgumentException e )
        {
            throw new IncorrectUsage( e.getMessage() );
        }

        File[] listFiles = outsideWorld.fileSystem().listFiles( destination );
        if ( listFiles != null && listFiles.length > 0 )
        {
            outsideWorld.stdOutLine( "Destination is not empty, doing incremental backup..." );
            try
            {
                backupService.doIncrementalBackup( address.getHost(), address.getPort(),
                        destination, ConsistencyCheck.NONE, timeout, config );
                done = true;
            }
            catch ( Exception e )
            {
                if ( fallbackToFull )
                {
                    outsideWorld.stdErrLine( "Incremental backup failed: " + e.getMessage() );
                    String renamed = renameExistingBackup( folder, name );
                    outsideWorld.stdErrLine( String.format( "Old backup renamed to '%s'.", renamed ) );
                }
                else
                {
                    throw new CommandFailed( "Backup failed: " + e.getMessage(), e );
                }
            }
        }

        if ( !done )
        {
            outsideWorld.stdOutLine( "Doing full backup..." );
            try
            {
                backupService.doFullBackup( address.getHost(), address.getPort(), destination,
                        ConsistencyCheck.NONE, config, timeout, false );
            }
            catch ( Exception e )
            {
                throw new CommandFailed( "Backup failed: " + e.getMessage(), e );
            }
        }

        if ( doConsistencyCheck )
        {
            try
            {
                outsideWorld.stdOutLine( "Doing consistency check..." );
                ConsistencyCheckService.Result ccResult = consistencyCheckService
                        .runFullConsistencyCheck( destination, config,
                                ProgressMonitorFactory.textual( outsideWorld.errorStream() ),
                                FormattedLogProvider.toOutputStream( outsideWorld.outStream() ),
                                outsideWorld.fileSystem(),
                                false, reportDir.toFile(),
                                new CheckConsistencyConfig( checkGraph, checkIndexes, checkLabelScanStore,
                                        checkPropertyOwners ) );

                if ( !ccResult.isSuccessful() )
                {
                    throw new CommandFailed( String.format( "Inconsistencies found. See '%s' for details.",
                            ccResult.reportFile() ), STATUS_CC_INCONSISTENT );
                }
            }
            catch ( Throwable e )
            {
                if ( e instanceof CommandFailed )
                {
                    throw (CommandFailed) e;
                }
                throw new CommandFailed( "Failed to do consistency check on backup: " + e.getMessage(), e,
                        STATUS_CC_ERROR );
            }
        }

        outsideWorld.stdOutLine( "Backup complete." );
    }

    private String renameExistingBackup( final Path folder, final String oldName ) throws CommandFailed
    {
        int i = 1;
        while ( i < MAX_OLD_BACKUPS )
        {
            String newName = oldName + ".err." + i;
            if ( outsideWorld.fileSystem().fileExists( folder.resolve( newName ).toFile() ) )
            {
                i++;
            }
            else
            {
                try
                {
                    outsideWorld.fileSystem().renameFile( folder.resolve( oldName ).toFile(),
                            folder.resolve( newName ).toFile() );
                    return newName;
                }
                catch ( IOException e )
                {
                    throw new CommandFailed( "Failed to move old backup out of the way: " + e.getMessage(), e );
                }
            }
        }
        throw new CommandFailed( "Failed to move old backup out of the way: too many old backups." );
    }

    private long parseTimeout( String[] args )
    {
        return Args.parse( args ).getDuration( "timeout", TimeUnit.MINUTES.toMillis( 20 ) );
    }

    private Config loadConfig( Optional<Path> additionalConfig ) throws CommandFailed
    {
        //noinspection unchecked
        return withAdditionalConfig( additionalConfig,
                ConfigLoader
                        .loadConfigWithConnectorsDisabled(
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

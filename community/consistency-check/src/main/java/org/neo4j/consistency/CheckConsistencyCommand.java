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
package org.neo4j.consistency;

import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import org.neo4j.cli.AbstractCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.Converters.DatabaseNameConverter;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.commandline.Util;
import org.neo4j.commandline.dbms.CannotWriteException;
import org.neo4j.commandline.dbms.LockChecker;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.ConfigUtils;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.NormalizedDatabaseName;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.kernel.internal.locker.FileLockException;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.VisibleForTesting;

import static java.lang.String.format;
import static org.neo4j.internal.helpers.Strings.joinAsLines;
import static org.neo4j.kernel.recovery.Recovery.isRecoveryRequired;
import static picocli.CommandLine.ArgGroup;
import static picocli.CommandLine.Command;

@Command(
        name = "check-consistency",
        header = "Check the consistency of a database.",
        description = "This command allows for checking the consistency of a database or a backup thereof. It cannot " +
                "be used with a database which is currently in use.%n" +
                "%n" +
                "All checks except 'check-graph' can be quite expensive so it may be useful to turn them off" +
                " for very large databases. Increasing the heap size can also be a good idea." +
                " See 'neo4j-admin help' for details."

)
public class CheckConsistencyCommand extends AbstractCommand
{
    @ArgGroup( multiplicity = "1" )
    private TargetOption target = new TargetOption();

    private static class TargetOption
    {
        @Option( names = "--database", description = "Name of the database to check.", converter = DatabaseNameConverter.class )
        private NormalizedDatabaseName database;

        @Option( names = "--backup", paramLabel = "<path>", description = "Path to backup to check consistency of. Cannot be used together with --database." )
        private Path backup;
    }

    @Option( names = "--additional-config", paramLabel = "<path>", description = "Configuration file to supply additional configuration in." )
    private Path additionalConfig;

    @Mixin
    private ConsistencyCheckOptions options;

    private final ConsistencyCheckService consistencyCheckService;

    public CheckConsistencyCommand( ExecutionContext ctx )
    {
        this( ctx, new ConsistencyCheckService() );
    }

    @VisibleForTesting
    public CheckConsistencyCommand( ExecutionContext ctx, ConsistencyCheckService consistencyCheckService )
    {
        super( ctx );
        this.consistencyCheckService = consistencyCheckService;
    }

    @Override
    public void execute()
    {
        if ( target.backup != null )
        {
            target.backup = target.backup.toAbsolutePath();
            if ( !Files.isDirectory( target.backup ) )
            {
                throw new CommandFailedException( "Report directory path doesn't exist or not a directory: " + target.backup );
            }
        }

        Config config = loadNeo4jConfig( ctx.homeDir(), ctx.confDir(), additionalConfig );
        var memoryTracker = EmptyMemoryTracker.INSTANCE;

        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {

            DatabaseLayout databaseLayout = Optional.ofNullable( target.backup )
                    .map( DatabaseLayout::ofFlat )
                    .orElseGet( () -> Neo4jLayout.of( config ).databaseLayout( target.database.name() ) );

            checkDatabaseExistence( databaseLayout );
            try ( Closeable ignored = LockChecker.checkDatabaseLock( databaseLayout ) )
            {
                checkDbState( databaseLayout, config, memoryTracker );
                // Only output progress indicator if a console receives the output
                ProgressMonitorFactory progressMonitorFactory = ProgressMonitorFactory.NONE;
                if ( System.console() != null )
                {
                    progressMonitorFactory = ProgressMonitorFactory.textual( System.out );
                }

                ConsistencyCheckService.Result consistencyCheckResult;
                try ( Log4jLogProvider logProvider = Util.configuredLogProvider( config, System.out ) )
                {
                    consistencyCheckResult = consistencyCheckService
                            .runFullConsistencyCheck( databaseLayout, config, progressMonitorFactory, logProvider, fileSystem,
                                    verbose, options.getReportDir().normalize(),
                                    new ConsistencyFlags( options.isCheckGraph(), options.isCheckIndexes(), options.isCheckIndexStructure(),
                                            options.isCheckLabelScanStore(), options.isCheckRelationshipTypeScanStore(), options.isCheckPropertyOwners() ) );
                }

                if ( !consistencyCheckResult.isSuccessful() )
                {
                    throw new CommandFailedException( format( "Inconsistencies found. See '%s' for details.",
                            consistencyCheckResult.reportFile() ) );
                }
            }
            catch ( FileLockException e )
            {
                throw new CommandFailedException( "The database is in use. Stop database '" + databaseLayout.getDatabaseName() + "' and try again.", e );
            }
            catch ( CannotWriteException e )
            {
                throw new CommandFailedException( "You do not have permission to check database consistency.", e );
            }
        }
        catch ( ConsistencyCheckIncompleteException | IOException e )
        {
            throw new CommandFailedException( "Consistency checking failed." + e.getMessage(), e );
        }
    }

    private void checkDatabaseExistence( DatabaseLayout databaseLayout )
    {
        try
        {
            Validators.CONTAINS_EXISTING_DATABASE.validate( databaseLayout.databaseDirectory() );
        }
        catch ( IllegalArgumentException e )
        {
            throw new CommandFailedException( "Database does not exist: " + databaseLayout.getDatabaseName(), e );
        }
    }

    private static void checkDbState( DatabaseLayout databaseLayout, Config additionalConfiguration, MemoryTracker memoryTracker )
    {
        if ( checkRecoveryState( databaseLayout, additionalConfiguration, memoryTracker ) )
        {
            throw new CommandFailedException( joinAsLines( "Active logical log detected, this might be a source of inconsistencies.",
                    "Please recover database before running the consistency check.",
                    "To perform recovery please start database and perform clean shutdown." ) );
        }
    }

    private static boolean checkRecoveryState( DatabaseLayout databaseLayout, Config additionalConfiguration, MemoryTracker memoryTracker )
    {
        try
        {
            return isRecoveryRequired( databaseLayout, additionalConfiguration, memoryTracker );
        }
        catch ( Exception e )
        {
            throw new CommandFailedException( "Failure when checking for recovery state: " + e.getMessage(), e );
        }
    }

    private static Config loadNeo4jConfig( Path homeDir, Path configDir, Path additionalConfig )
    {
        Config cfg = Config.newBuilder()
                .fromFileNoThrow( configDir.resolve( Config.DEFAULT_CONFIG_FILE_NAME ) )
                .fromFileNoThrow( additionalConfig )
                .set( GraphDatabaseSettings.neo4j_home, homeDir )
                .build();
        ConfigUtils.disableAllConnectors( cfg );
        return cfg;
    }
}

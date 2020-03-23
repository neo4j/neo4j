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
package org.neo4j.commandline.dbms;

import picocli.CommandLine;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.cli.AbstractCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.Converters.DatabaseNameConverter;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.commandline.dbms.storeutil.StoreCopy;
import org.neo4j.commandline.dbms.storeutil.StoreCopy.FormatEnum;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.ConfigUtils;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.NormalizedDatabaseName;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.kernel.internal.locker.FileLockException;

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.databases_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.internal.helpers.Strings.joinAsLines;
import static org.neo4j.internal.helpers.collection.Iterables.stream;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.allFormats;
import static org.neo4j.kernel.recovery.Recovery.isRecoveryRequired;
import static picocli.CommandLine.ArgGroup;
import static picocli.CommandLine.Command;
import static picocli.CommandLine.Help.Visibility.NEVER;
import static picocli.CommandLine.Option;

@Command(
        name = "copy",
        header = "Copy a database and optionally apply filters.",
        description = "This command will create a copy of a database."
)
public class StoreCopyCommand extends AbstractCommand
{
    @ArgGroup( multiplicity = "1" )
    private SourceOption source = new SourceOption();

    private static class SourceOption
    {
        @Option( names = "--from-database", description = "Name of database to copy from.", required = true, converter = DatabaseNameConverter.class )
        private NormalizedDatabaseName database;
        @Option( names = "--from-path", description = "Path to the database to copy from.", required = true )
        private Path path;
    }

    @Option(
            names = "--from-path-tx",
            description = "Path to the transaction files, if they are not in the same folder as '--from-path'.",
            paramLabel = "<path>"
    )
    private Path sourceTxLogs;

    @Option( names = "--to-database", description = "Name of database to copy to.", required = true, converter = DatabaseNameConverter.class )
    private NormalizedDatabaseName database;

    // --force
    @Option( names = "--force", description = "Force the command to run even if the integrity of the database can not be verified." )
    private boolean force;

    @Option(
            completionCandidates = StoreFormatCandidates.class,
            names = "--to-format",
            defaultValue = "same",
            converter = FormatNameConverter.class,
            description = "Set the format for the new database. Must be one of ${COMPLETION-CANDIDATES}. 'same' will use the same format as the source. " +
                    "WARNING: 'high_limit' format is only available in enterprise edition. If you go from 'high_limit' to 'standard' there is " +
                    "no validation that the data will actually fit."
    )
    private FormatEnum format;

    @Option(
            names = "--delete-nodes-with-labels",
            description = "A comma separated list of labels. All nodes that have ANY of the specified labels will be deleted.",
            split = ",",
            paramLabel = "<label>",
            showDefaultValue = NEVER
    )
    private List<String> deleteNodesWithLabels = new ArrayList<>();

    @Option(
            names = "--skip-labels",
            description = "A comma separated list of labels to ignore.",
            split = ",",
            paramLabel = "<label>",
            showDefaultValue = NEVER
    )
    private List<String> skipLabels = new ArrayList<>();

    @Option(
            names = "--skip-properties",
            description = "A comma separated list of property keys to ignore.",
            split = ",",
            paramLabel = "<property>",
            showDefaultValue = NEVER
    )
    private List<String> skipProperties = new ArrayList<>();

    @Option(
            names = "--skip-relationships",
            description = "A comma separated list of relationships to ignore.",
            split = ",",
            paramLabel = "<relationship>",
            showDefaultValue = NEVER
    )
    private List<String> skipRelationships = new ArrayList<>();

    @Option( names = "--from-pagecache", paramLabel = "<size>", defaultValue = "8m", description = "The size of the page cache to use for reading." )
    private String fromPageCacheMemory;

    @Option( names = "--to-pagecache", paramLabel = "<size>", defaultValue = "8m", description = "The size of the page cache to use for writing." )
    private String toPageCacheMemory;

    public StoreCopyCommand( ExecutionContext ctx )
    {
        super( ctx );
    }

    @Override
    public void execute() throws Exception
    {
        Config config = buildConfig();
        DatabaseLayout fromDatabaseLayout = getFromDatabaseLayout( config );

        validateSource( fromDatabaseLayout );

        DatabaseLayout toDatabaseLayout = Neo4jLayout.of( config ).databaseLayout( database.name() );

        validateTarget( toDatabaseLayout );

        try ( Closeable ignored = LockChecker.checkDatabaseLock( fromDatabaseLayout ) )
        {
            if ( !force )
            {
                checkDbState( fromDatabaseLayout, config );
            }
            try ( Closeable ignored2 = LockChecker.checkDatabaseLock( toDatabaseLayout )  )
            {
                StoreCopy copy =
                        new StoreCopy( fromDatabaseLayout, config, format, deleteNodesWithLabels, skipLabels, skipProperties, skipRelationships, verbose,
                                ctx.out() );
                try
                {
                    copy.copyTo( toDatabaseLayout, fromPageCacheMemory, toPageCacheMemory );
                }
                catch ( Exception e )
                {
                    throw new CommandFailedException( "There was a problem during copy.", e );
                }
            }
            catch ( FileLockException e )
            {
                throw new CommandFailedException( "Unable to lock destination.", e );
            }
        }
        catch ( FileLockException e )
        {
            throw new CommandFailedException( "The database is in use. Stop database '" + fromDatabaseLayout.getDatabaseName() + "' and try again.", e );
        }
    }

    private DatabaseLayout getFromDatabaseLayout( Config config )
    {
        if ( source.path != null )
        {
            source.path = source.path.toAbsolutePath();
            if ( !Files.isDirectory( source.path ) )
            {
                throw new CommandFailedException( "The path doesn't exist or not a directory: " + source.path );
            }
            if ( new TransactionLogFilesHelper( ctx.fs(), source.path.toFile() ).getLogFiles().length > 0 )
            {
                // Transaction logs are in the same directory
                return DatabaseLayout.ofFlat( source.path.toFile() );
            }
            else
            {
                if ( sourceTxLogs == null )
                {
                    // Transaction logs are in the same directory and not configured, unable to continue
                    throw new CommandFailedException( "Unable to find transaction logs, please specify the location with '--from-path-tx'." );
                }

                Path databaseName = source.path.getFileName();
                if ( !databaseName.equals( sourceTxLogs.getFileName() ) )
                {
                    throw new CommandFailedException( "The directory with data and the directory with transaction logs need to have the same name." );
                }

                sourceTxLogs = sourceTxLogs.toAbsolutePath();
                Config cfg = Config.newBuilder()
                        .set( default_database, databaseName.toString() )
                        .set( neo4j_home, source.path.getParent() )
                        .set( databases_root_path, source.path.getParent() )
                        .set( transaction_logs_root_path, sourceTxLogs.getParent() )
                        .build();
                return DatabaseLayout.of( cfg );
            }
        }
        else
        {
            return Neo4jLayout.of( config ).databaseLayout( source.database.name() );
        }
    }

    private static void validateSource( DatabaseLayout fromDatabaseLayout )
    {
        try
        {
            Validators.CONTAINS_EXISTING_DATABASE.validate( fromDatabaseLayout.databaseDirectory() );
        }
        catch ( IllegalArgumentException e )
        {
            throw new CommandFailedException( "Database does not exist: " + fromDatabaseLayout.getDatabaseName(), e );
        }
    }

    private static void validateTarget( DatabaseLayout toDatabaseLayout )
    {
        File targetFile = toDatabaseLayout.databaseDirectory();
        if ( targetFile.exists() )
        {
            if ( targetFile.isDirectory() )
            {
                String[] files = targetFile.list();
                if ( files == null || files.length > 0 )
                {
                    throw new CommandFailedException( "The directory is not empty: " + targetFile.getAbsolutePath() );
                }
            }
            else
            {
                throw new CommandFailedException( "Specified path is a file: " + targetFile.getAbsolutePath() );
            }
        }
        else
        {
            try
            {
                Files.createDirectories( targetFile.toPath() );
            }
            catch ( IOException e )
            {
                throw new CommandFailedException( "Unable to create directory: " + targetFile.getAbsolutePath() );
            }
        }
    }

    private static void checkDbState( DatabaseLayout databaseLayout, Config additionalConfiguration )
    {
        if ( checkRecoveryState( databaseLayout, additionalConfiguration ) )
        {
            throw new CommandFailedException( joinAsLines( "The database " + databaseLayout.getDatabaseName() + "  was not shut down properly.",
                    "Please perform a recovery by starting and stopping the database.",
                    "If recovery is not possible, you can force the command to continue with the '--force' flag.") );
        }
    }

    private static boolean checkRecoveryState( DatabaseLayout databaseLayout, Config additionalConfiguration )
    {
        try
        {
            return isRecoveryRequired( databaseLayout, additionalConfiguration );
        }
        catch ( Exception e )
        {
            throw new CommandFailedException( "Failure when checking for recovery state: '%s'." + e.getMessage(), e );
        }
    }

    private Config buildConfig()
    {
        Config cfg = Config.newBuilder()
                .fromFileNoThrow( ctx.confDir().resolve( Config.DEFAULT_CONFIG_FILE_NAME ) )
                .set( GraphDatabaseSettings.neo4j_home, ctx.homeDir() ).build();
        ConfigUtils.disableAllConnectors( cfg );
        return cfg;
    }

    private static boolean isHighLimitFormatMissing()
    {
        return stream( allFormats() ).noneMatch( format -> "high_limit".equals( format.name() ) );
    }

    public static class FormatNameConverter implements CommandLine.ITypeConverter<FormatEnum>
    {
        @Override
        public FormatEnum convert( String name )
        {
            try
            {
                var format = FormatEnum.valueOf( name );
                if ( format == FormatEnum.high_limit && isHighLimitFormatMissing() )
                {
                    throw new CommandLine.TypeConversionException( "High limit format available only in enterprise edition." );
                }
                return format;
            }
            catch ( Exception e )
            {
                throw new CommandLine.TypeConversionException( format( "Invalid database format name '%s'. (%s)", name, e ) );
            }
        }
    }

    public static class StoreFormatCandidates implements Iterable<String>
    {
        @Override
        public Iterator<String> iterator()
        {
            List<String> storeFormats = new ArrayList<>( Arrays.stream( FormatEnum.values() ).map( Enum::name ).collect( Collectors.toList() ) );
            if ( isHighLimitFormatMissing() )
            {
                storeFormats.remove( FormatEnum.high_limit.name() );
            }
            return storeFormats.iterator();
        }
    }
}

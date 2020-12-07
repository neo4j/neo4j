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

import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.neo4j.cli.AbstractCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.kernel.internal.locker.FileLockException;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreVersion;

import static java.lang.String.format;
import static java.util.Comparator.comparing;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;
import static org.neo4j.kernel.recovery.Recovery.isRecoveryRequired;
import static picocli.CommandLine.Command;

@Command(
        name = "store-info",
        header = "Print information about a Neo4j database store.",
        description = "Print information about a Neo4j database store, such as what version of Neo4j created it."
)
public class StoreInfoCommand extends AbstractCommand
{
    @Option( names = "--structured", arity = "0", description = "Return result structured as json" )
    private boolean structured;

    @Option( names = "--all", arity = "0", description = "Return store info for all databases at provided path" )
    private boolean all;

    @Parameters( description = "Path to database store files, or databases directory if --all option is used" )
    private Path path;

    public StoreInfoCommand( ExecutionContext ctx )
    {
        super( ctx );
    }

    @Override
    public void execute()
    {
        var storageEngineFactory = StorageEngineFactory.selectStorageEngine();
        var config = CommandHelpers.buildConfig( ctx );
        var neo4jLayout = Neo4jLayout.of( config );
        try ( var fs = ctx.fs();
              var jobScheduler = createInitialisedScheduler();
              var pageCache = StandalonePageCacheFactory.createPageCache( fs, jobScheduler, PageCacheTracer.NULL ) )
        {
            validatePath( fs, all, path, neo4jLayout );
            if ( all )
            {
                var collector = structured ?
                                Collectors.joining( ",", "[", "]" ) :
                                Collectors.joining( System.lineSeparator() + System.lineSeparator() );
                var result = Arrays.stream( fs.listFiles( path ) )
                        .sorted( comparing( Path::getFileName ) )
                        .map( dbPath -> neo4jLayout.databaseLayout( dbPath.getFileName().toString() ) )
                        .filter( dbLayout -> Validators.isExistingDatabase( fs, dbLayout ) )
                        .map( dbLayout -> printInfo( fs, dbLayout, pageCache, storageEngineFactory, config, structured, true ) )
                        .collect( collector );
                ctx.out().println( result );
            }
            else
            {
                var databaseLayout = neo4jLayout.databaseLayout( path.getFileName().toString() );
                ctx.out().println( printInfo( fs, databaseLayout, pageCache, storageEngineFactory, config, structured, false ) );
            }
        }
        catch ( CommandFailedException e )
        {
            throw e;
        }
        catch ( Exception e )
        {
            throw new CommandFailedException( format( "Failed to execute command: '%s'.", e.getMessage() ), e );
        }
    }

    private static void validatePath( FileSystemAbstraction fs, boolean all, Path storePath, Neo4jLayout neo4jLayout )
    {
        if ( !fs.isDirectory( storePath ) )
        {
            throw new IllegalArgumentException( format( "Provided path %s must point to a directory.", storePath.toAbsolutePath() ) );
        }

        var dirName = storePath.getFileName().toString();
        var databaseLayout = neo4jLayout.databaseLayout( dirName );
        var pathIsDatabase = Validators.isExistingDatabase( fs, databaseLayout );
        if ( all && pathIsDatabase )
        {
            throw new IllegalArgumentException( format( "You used the --all option but directory %s contains the store files of a single database, " +
                                                "rather than several database directories.", storePath.toAbsolutePath() ) );
        }
        else if ( !all && !pathIsDatabase )
        {
            throw new IllegalArgumentException( format( "Directory %s does not contain the store files of a database, but you did not use the --all option.",
                                                        storePath.toAbsolutePath() ) );
        }
    }

    private static String printInfo( FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache, StorageEngineFactory storageEngineFactory,
            Config config, boolean structured, boolean failSilently )
    {
        var memoryTracker = EmptyMemoryTracker.INSTANCE;
        try ( var ignored = LockChecker.checkDatabaseLock( databaseLayout ) )
        {
            var storeVersionCheck = storageEngineFactory.versionCheck( fs, databaseLayout, Config.defaults(), pageCache,
                    NullLogService.getInstance(), PageCacheTracer.NULL );
            var storeVersion = storeVersionCheck.storeVersion( PageCursorTracer.NULL )
                    .orElseThrow( () ->
                            new CommandFailedException( format( "Could not find version metadata in store '%s'", databaseLayout.databaseDirectory() ) ) );

            var versionInformation = storageEngineFactory.versionInformation( storeVersion );

            var recoveryRequired = checkRecoveryState( fs, databaseLayout, config, memoryTracker );
            var txIdStore = storageEngineFactory.readOnlyTransactionIdStore( fs, databaseLayout, pageCache, PageCursorTracer.NULL );
            var lastTxId = txIdStore.getLastCommittedTransactionId(); // Latest committed tx id found in metadata store. May be behind if recovery is required.
            var successorString = versionInformation.successor().map( StoreVersion::introductionNeo4jVersion ).orElse( null );

            var storeInfo = StoreInfo.notInUseResult( databaseLayout.getDatabaseName(),
                    storeVersion,
                    versionInformation.introductionNeo4jVersion(),
                    successorString,
                    lastTxId,
                    recoveryRequired );

            return storeInfo.print( structured );
        }
        catch ( FileLockException e )
        {
            if ( !failSilently )
            {
                throw new CommandFailedException( format( "Failed to execute command as the database '%s' is in use. " +
                                                          "Please stop it and try again.", databaseLayout.getDatabaseName() ), e );
            }
            return StoreInfo.inUseResult( databaseLayout.getDatabaseName() ).print( structured );
        }
        catch ( CommandFailedException e )
        {
            throw e;
        }
        catch ( Exception e )
        {
            throw new CommandFailedException( format( "Failed to execute command: '%s'.", e.getMessage() ), e );
        }
    }

    private static boolean checkRecoveryState( FileSystemAbstraction fs, DatabaseLayout databaseLayout, Config config, MemoryTracker memoryTracker )
    {
        try
        {
            return isRecoveryRequired( fs, databaseLayout, config, memoryTracker );
        }
        catch ( Exception e )
        {
            throw new CommandFailedException( format( "Failed to execute command when checking for recovery state: '%s'.", e.getMessage() ), e );
        }
    }

    private static class StoreInfo
    {
        private final String databaseName;
        private final String storeFormat;
        private final String storeFormatIntroduced;
        private final String storeFormatSuperseded;
        private final long lastCommittedTransaction;
        private final boolean recoveryRequired;
        private final boolean inUse;

        static StoreInfo inUseResult( String databaseName )
        {
            return new StoreInfo( databaseName, true, null, null, null, -1, true );
        }

        static StoreInfo notInUseResult( String databaseName, String storeFormat, String storeFormatIntroduced, String storeFormatSuperseded,
                long lastCommittedTransaction, boolean recoveryRequired )
        {
            return new StoreInfo( databaseName, false, storeFormat, storeFormatIntroduced, storeFormatSuperseded, lastCommittedTransaction, recoveryRequired );
        }

        private StoreInfo( String databaseName, boolean inUse, String storeFormat, String storeFormatIntroduced, String storeFormatSuperseded,
                long lastCommittedTransaction, boolean recoveryRequired )
        {
            this.databaseName = databaseName;
            this.storeFormat = storeFormat;
            this.storeFormatIntroduced = storeFormatIntroduced;
            this.storeFormatSuperseded = storeFormatSuperseded;
            this.lastCommittedTransaction = lastCommittedTransaction;
            this.recoveryRequired = recoveryRequired;
            this.inUse = inUse;
        }

        List<Pair<InfoType,String>> printFields()
        {
            return List.of(
                    Pair.of( InfoType.DatabaseName, databaseName ),
                    Pair.of( InfoType.InUse, Boolean.toString( inUse ) ),
                    Pair.of( InfoType.StoreFormat, storeFormat ),
                    Pair.of( InfoType.StoreFormatIntroduced, storeFormatIntroduced ),
                    Pair.of( InfoType.StoreFormatSuperseded, storeFormatSuperseded ),
                    Pair.of( InfoType.LastCommittedTransaction, Long.toString( lastCommittedTransaction ) ),
                    Pair.of( InfoType.RecoveryRequired, Boolean.toString( recoveryRequired ) ) );
        }

        String print( boolean structured )
        {
            if ( !structured )
            {
                return printFields().stream()
                                    .filter( p -> Objects.nonNull( p.other() ) )
                                    .map( p -> p.first().justifiedPretty( p.other() ) )
                                    .collect( Collectors.joining( System.lineSeparator() ) );
            }
            return printFields().stream()
                                .map( p -> p.first().structuredJson( p.other() ) )
                                .collect( Collectors.joining( ",", "{", "}" ) );
        }
    }

    private enum InfoType
    {
        InUse( "Database in use", "inUse" ),
        DatabaseName( "Database name", "databaseName" ),
        StoreFormat( "Store format version", "storeFormat" ),
        StoreFormatIntroduced( "Store format introduced in", "storeFormatIntroduced" ),
        StoreFormatSuperseded( "Store format superseded in", "storeFormatSuperseded" ),
        LastCommittedTransaction( "Last committed transaction id", "lastCommittedTransaction" ),
        RecoveryRequired( "Store needs recovery", "recoveryRequired" );

        private final String prettyPrint;
        private final String jsonKey;

        InfoType( String prettyPrint, String jsonKey )
        {
            this.prettyPrint = prettyPrint;
            this.jsonKey = jsonKey;
        }

        String justifiedPretty( String value )
        {
            var nullSafeValue = value == null ? "N/A" : value;
            var leftJustifiedFmt = "%-30s%s";
            return String.format( leftJustifiedFmt, prettyPrint + ":", nullSafeValue );
        }

        String structuredJson( String value )
        {
            var kFmt = "\"%s\":";
            var kvFmt = kFmt + "\"%s\"";
            return value == null ? String.format( kFmt + "null", jsonKey ) : String.format( kvFmt, jsonKey, value );
        }
    }
}

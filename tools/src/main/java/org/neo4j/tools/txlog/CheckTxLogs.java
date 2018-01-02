/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.tools.txlog;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.helpers.Args;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.LogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.test.LogTestUtils;
import org.neo4j.tools.txlog.checktypes.CheckType;
import org.neo4j.tools.txlog.checktypes.CheckTypes;

/**
 * Tool that verifies consistency of transaction logs.
 *
 * Transaction log is considered consistent when every command's before state is the same as after state for
 * corresponding record in previously committed transaction.
 *
 * Tool expects a single argument - directory with transaction logs.
 * It then simply iterates over all commands in those logs, compares before state for current record with previously
 * seen after state and stores after state for current record, if before state is consistent.
 */
public class CheckTxLogs
{
    private static final String HELP_FLAG = "help";
    private static final String CHECKS = "checks";
    private static final String SEPARATOR = ",";

    private final PrintStream out;
    private final FileSystemAbstraction fs;

    public CheckTxLogs( PrintStream out, FileSystemAbstraction fs )
    {
        this.out = out;
        this.fs = fs;
    }

    public static void main( String[] args ) throws Exception
    {
        Args arguments = Args.withFlags( HELP_FLAG ).parse( args );
        PrintStream printStream = System.out;
        if ( arguments.getBoolean( HELP_FLAG ) )
        {
            printUsageAndExit( printStream );
        }
        CheckType[] checkTypes = parseChecks( arguments );
        File dir = parseDir( printStream, arguments );

        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        PhysicalLogFiles logFiles = new PhysicalLogFiles( dir, fs );
        int numberOfLogFilesFound = (int) (logFiles.getHighestLogVersion() - logFiles.getLowestLogVersion() + 1);
        System.out.println( "Found " + numberOfLogFilesFound + " log files to verify" );

        CheckTxLogs tool = new CheckTxLogs( System.out, fs );
        if ( !tool.scan( dir, new PrintingInconsistenciesHandler( printStream ), checkTypes ) )
        {
            System.exit( 1 );
        }
    }

    // used in other projects do not remove!
    public boolean checkAll( File storeDirectory ) throws IOException
    {
        return scan( storeDirectory, new PrintingInconsistenciesHandler( out ), CheckTypes.CHECK_TYPES );
    }

    boolean scan( File storeDirectory, InconsistenciesHandler handler, CheckType<?,?>... checkTypes ) throws IOException
    {
        boolean success = true;
        for ( CheckType<?,?> checkType : checkTypes )
        {
            success &= scan( storeDirectory, handler, checkType );
        }
        return success;
    }

    class CommandAndLogVersion
    {
        StorageCommand command;
        long logVersion;

        CommandAndLogVersion( StorageCommand command, long logVersion )
        {
            this.command = command;
            this.logVersion = logVersion;
        }
    }

    private <C extends Command, R extends AbstractBaseRecord> boolean scan(
            File storeDirectory, InconsistenciesHandler handler, CheckType<C,R> check ) throws IOException
    {
        out.println( "Checking logs for " + check.name() + " inconsistencies" );
        CommittedRecords<R> state = new CommittedRecords<>( check );

        List<CommandAndLogVersion> txCommands = new ArrayList<>();
        boolean validLogs = true;
        long commandsRead = 0;
        try ( LogEntryCursor logEntryCursor = LogTestUtils.openLogs( fs, storeDirectory ) )
        {
            while ( logEntryCursor.next() )
            {
                LogEntry entry = logEntryCursor.get();
                if ( entry instanceof LogEntryCommand )
                {
                    StorageCommand command = ((LogEntryCommand) entry).getXaCommand();
                    if ( check.commandClass().isInstance( command ) )
                    {
                        long logVersion = logEntryCursor.getCurrentLogVersion();
                        txCommands.add( new CommandAndLogVersion( command, logVersion ) );
                    }
                }
                else if ( entry instanceof LogEntryCommit )
                {
                    long txId = ((LogEntryCommit) entry).getTxId();
                    for ( CommandAndLogVersion txCommand : txCommands )
                    {
                        validLogs &= checkAndHandleInconsistencies( txCommand, check, state, txId, handler );
                    }
                    txCommands.clear();
                }
                commandsRead++;
            }
        }
        out.println( "Processed " + storeDirectory.getCanonicalPath() + " with " + commandsRead + " commands" );
        out.println( state );

        if ( !txCommands.isEmpty() )
        {
            out.println( "Found " + txCommands.size() + " uncommitted commands at the end." );
            for ( CommandAndLogVersion txCommand : txCommands )
            {
                validLogs &= checkAndHandleInconsistencies( txCommand, check, state, -1, handler );
            }
            txCommands.clear();
        }

        return validLogs;
    }

    private <C extends Command, R extends AbstractBaseRecord> boolean checkAndHandleInconsistencies(
            CommandAndLogVersion txCommand, CheckType<C,R> check,
            CommittedRecords<R> state, long txId, InconsistenciesHandler handler )
    {
        C command = check.commandClass().cast( txCommand.command );

        R before = check.before( command );
        R after = check.after( command );

        assert before.getId() == after.getId();

        RecordInfo<R> lastSeen = state.get( after.getId() );

        boolean isValidRecord = (lastSeen == null) || check.equal( before, lastSeen.record() );
        if ( !isValidRecord )
        {
            handler.reportInconsistentCommand( lastSeen, new RecordInfo<>( before, txCommand.logVersion, txId ) );
        }

        state.put( after, txCommand.logVersion, txId );

        return isValidRecord;
    }

    private static CheckType[] parseChecks( Args arguments )
    {
        String checks = arguments.get( CHECKS );
        if ( checks == null )
        {
            return CheckTypes.CHECK_TYPES;
        }

        return Stream.of( checks.split( SEPARATOR ) )
                .map( CheckTypes::fromName )
                .toArray( CheckType<?,?>[]::new );
    }

    private static File parseDir( PrintStream printStream, Args args )
    {
        if ( args.orphans().size() != 1 )
        {
            printUsageAndExit(printStream);
        }
        File dir = new File( args.orphans().get( 0 ) );
        if ( !dir.isDirectory() )
        {
            printStream.println( "Invalid directory: '" + dir + "'" );
            printUsageAndExit(printStream);
        }
        return dir;
    }

    private static void printUsageAndExit( PrintStream out )
    {
        out.println( "Tool expects single argument - directory with tx logs" );
        out.println( "Usage:" );
        out.println( "\t./checkTxLogs [options] <directory>" );
        out.println( "Options:" );
        out.println( "\t--help\t\tprints this description" );
        out.println( "\t--checks='checkname[,...]'\t\tthe list of checks to perform. Checks available: " +
                            Arrays.stream( CheckTypes.CHECK_TYPES )
                                    .map( CheckType::name )
                                    .collect( Collectors.joining( SEPARATOR ) ) );
        System.exit( 1 );
    }
}

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
package org.neo4j.tools.txlog;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.helpers.Args;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.storemigration.LogFiles;
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

    private final FileSystemAbstraction fs;

    public CheckTxLogs( FileSystemAbstraction fs )
    {
        this.fs = fs;
    }

    public static void main( String[] args ) throws Exception
    {
        Args arguments = Args.withFlags( HELP_FLAG ).parse( args );
        if ( arguments.getBoolean( HELP_FLAG ) )
        {
            printUsageAndExit();
        }
        CheckType[] checkTypes = parseChecks( arguments );
        File dir = parseDir( arguments );

        File[] logs = txLogsIn( dir );
        System.out.println( "Found " + logs.length + " log files to verify" );

        CheckTxLogs tool = new CheckTxLogs( new DefaultFileSystemAbstraction() );

        tool.scan( logs, new PrintingInconsistenciesHandler(), checkTypes );
    }

    void scan( File[] logs, InconsistenciesHandler handler, CheckType<?,?>... checkTypes ) throws IOException
    {
        for ( CheckType<?,?> checkType : checkTypes )
        {
            scan( logs, handler, checkType );
        }
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

    private <C extends Command, R extends AbstractBaseRecord> void scan(
            File[] logs, InconsistenciesHandler handler, CheckType<C,R> check ) throws IOException
    {
        System.out.println( "Checking logs for " + check.name() + " inconsistencies" );
        CommittedRecords<R> state = new CommittedRecords<>( check );

        List<CommandAndLogVersion> txCommands = new ArrayList<>();
        for ( File log : logs )
        {
            long commandsRead = 0;
            long logVersion = PhysicalLogFiles.getLogVersion( log );

            try ( LogEntryCursor logEntryCursor = LogTestUtils.openLog( fs, log ) )
            {
                while ( logEntryCursor.next() )
                {
                    LogEntry entry = logEntryCursor.get();
                    if ( entry instanceof LogEntryCommand )
                    {
                        StorageCommand command = ((LogEntryCommand) entry).getXaCommand();
                        if ( check.commandClass().isInstance( command ) )
                        {
                            txCommands.add( new CommandAndLogVersion( command, logVersion ) );
                        }
                    }
                    else if ( entry instanceof LogEntryCommit )
                    {
                        long txId = ((LogEntryCommit) entry).getTxId();
                        for ( CommandAndLogVersion txCommand : txCommands )
                        {
                            checkAndHandleInconsistencies( txCommand, check, state, txId, handler );
                        }
                        txCommands.clear();
                    }
                    commandsRead++;
                }
            }
            System.out.println( "Processed " + log.getCanonicalPath() + " with " + commandsRead + " commands" );
            System.out.println( state );
        }

        if ( !txCommands.isEmpty() )
        {
            System.out.println( "Found " + txCommands.size() + " uncommitted commands at the end." );
            for ( CommandAndLogVersion txCommand : txCommands )
            {
                checkAndHandleInconsistencies( txCommand, check, state, -1, handler );
            }
            txCommands.clear();
        }
    }

    private <C extends Command, R extends AbstractBaseRecord> void checkAndHandleInconsistencies( CommandAndLogVersion txCommand, CheckType<C,R> check,
            CommittedRecords<R> state, long txId, InconsistenciesHandler handler )
    {
        C command = check.commandClass().cast( txCommand.command );

        R before = check.before( command );
        R after = check.after( command );

        assert before.getId() == after.getId();

        RecordInfo<R> lastSeen = state.get( after.getId() );

        if ( lastSeen != null && !check.equal( before, lastSeen.record() ) )
        {
            handler.reportInconsistentCommand( lastSeen, new RecordInfo<>( before, txCommand.logVersion, txId ) );
        }

        state.put( after, txCommand.logVersion, txId );
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

    private static File parseDir( Args args )
    {
        if ( args.orphans().size() != 1 )
        {
            printUsageAndExit();
        }
        File dir = new File( args.orphans().get( 0 ) );
        if ( !dir.isDirectory() )
        {
            System.out.println( "Invalid directory: '" + dir + "'" );
            printUsageAndExit();
        }
        return dir;
    }

    private static File[] txLogsIn( File dir )
    {
        File[] logs = dir.listFiles( LogFiles.FILENAME_FILTER );
        Arrays.sort( logs, ( f1, f2 ) -> {
            long f1Version = PhysicalLogFiles.getLogVersion( f1 );
            long f2Version = PhysicalLogFiles.getLogVersion( f2 );
            return Long.compare( f1Version, f2Version );
        } );
        return logs;
    }

    private static void printUsageAndExit()
    {
        System.out.println( "Tool expects single argument - directory with tx logs" );
        System.out.println( "Usage:" );
        System.out.println( "\t./checkTxLogs [options] <directory>" );
        System.out.println( "Options:" );
        System.out.println( "\t--help\t\tprints this description" );
        System.out.println( "\t--checks='checkname[,...]'\t\tthe list of checks to perform. Checks available: " +
                            Arrays.stream( CheckTypes.CHECK_TYPES )
                                    .map( CheckType::name )
                                    .collect( Collectors.joining( SEPARATOR ) ) );
        System.exit( 1 );
    }
}

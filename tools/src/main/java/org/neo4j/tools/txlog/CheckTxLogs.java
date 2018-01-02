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
import org.neo4j.helpers.Args;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.record.Abstract64BitRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.LogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.test.LogTestUtils;

/**
 * Tool that verifies consistency of transaction logs.
 * <p/>
 * Transaction log is considered consistent when every command's before state is the same as after state for
 * corresponding record in previously committed transaction.
 * <p/>
 * Tool expects a single argument - directory with transaction logs.
 * It then simply iterates over all commands in those logs, compares before state for current record with previously
 * seen after state and stores after state for current record, if before state is consistent.
 */
public class CheckTxLogs
{
    private static final String HELP_FLAG = "help";

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
        if ( arguments.getBoolean( HELP_FLAG ) )
        {
            printUsageAndExit( System.out );
        }
        File dir = parseDir( System.out, arguments );

        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        PhysicalLogFiles logFiles = new PhysicalLogFiles( dir, fs );
        int numberOfLogFilesFound = (int) (logFiles.getHighestLogVersion() - logFiles.getLowestLogVersion() + 1);
        System.out.println( "Found " + numberOfLogFilesFound + " log files to verify" );

        CheckTxLogs tool = new CheckTxLogs( System.out, fs );
        if ( !tool.checkAll( dir ) )
        {
            System.exit( 1 );
        }
    }

    public boolean checkAll( File storeDirectory ) throws IOException
    {
        boolean success = true;

        success &= scan( storeDirectory, CheckType.NODE, new PrintingInconsistenciesHandler( out ) );
        success &= scan( storeDirectory, CheckType.PROPERTY, new PrintingInconsistenciesHandler( out ) );

        return success;
    }

    <C extends Command, R extends Abstract64BitRecord> boolean scan( File storeDirectory,
            CheckType<C,R> check, InconsistenciesHandler handler ) throws IOException
    {
        out.println( "Checking logs for " + check.name() + " inconsistencies" );

        CommittedRecords<R> state = new CommittedRecords<>( check );

        boolean validLogs = true;
        long commandsRead = 0;
        try ( LogEntryCursor logEntryCursor = LogTestUtils.openLogs( fs, storeDirectory ) )
        {
            while ( logEntryCursor.next() )
            {
                LogEntry entry = logEntryCursor.get();
                if ( entry instanceof LogEntryCommand )
                {
                    Command command = ((LogEntryCommand) entry).getXaCommand();
                    if ( check.commandClass().isInstance( command ) )
                    {
                        long logVersion = logEntryCursor.getCurrentLogVersion();
                        C cmd = check.commandClass().cast( command );
                        validLogs &= process( cmd, check, state, logVersion, handler );
                    }
                }
                commandsRead++;
            }
        }
        out.println( "Processed " + storeDirectory.getCanonicalPath() + " with " + commandsRead + " commands" );
        out.println( state );

        return validLogs;
    }

    private <C extends Command, R extends Abstract64BitRecord> boolean process( C command, CheckType<C,R> check,
            CommittedRecords<R> state, long logVersion, InconsistenciesHandler handler )
    {
        R before = check.before( command );
        R after = check.after( command );

        boolean isValid = state.isValid( before );
        if ( !isValid )
        {
            LogRecord<R> seen = state.get( before.getId() );
            LogRecord<R> current = new LogRecord<>( before, logVersion );

            handler.handle( seen, current );
        }

        state.put( after, logVersion );

        return isValid;
    }

    private static File parseDir( PrintStream out, Args args )
    {
        if ( args.orphans().size() != 1 )
        {
            printUsageAndExit( out );
        }
        File dir = new File( args.orphans().get( 0 ) );
        if ( !dir.isDirectory() )
        {
            out.println( "Invalid directory: '" + dir + "'" );
            printUsageAndExit( out );
        }
        return dir;
    }

    private static void printUsageAndExit( PrintStream out )
    {
        out.println( "Tool expects single argument - directory with tx logs" );
        out.println( "Example:\n\t./checkTxLogs <directory containing neostore.transaction.db files>" );
        System.exit( 1 );
    }
}

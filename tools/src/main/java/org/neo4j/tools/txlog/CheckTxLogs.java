/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import java.util.Arrays;
import java.util.Comparator;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.record.Abstract64BitRecord;
import org.neo4j.kernel.impl.storemigration.LogFiles;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.tools.LogEntryIterator;

public class CheckTxLogs
{
    interface InconsistenciesHandler
    {
        void handle( LogRecord<?> seen, LogRecord<?> current );
    }

    private final FileSystemAbstraction fs;
    private final InconsistenciesHandler inconsistenciesHandler;
    private final PrintStream out;

    public CheckTxLogs( FileSystemAbstraction fs, InconsistenciesHandler inconsistenciesHandler, PrintStream out )
    {
        this.fs = fs;
        this.inconsistenciesHandler = inconsistenciesHandler;
        this.out = out;
    }

    public static void main( String[] args ) throws Exception
    {
        File dir = parseDir( args, System.out );

        File[] logs = txLogsIn( dir );
        System.out.println( "Found " + logs.length + " log files to verify" );

        CheckTxLogs checker = new CheckTxLogs( new DefaultFileSystemAbstraction(), throwIfInconsistent(), System.out );

        checker.scan( logs, CheckType.NODE );
        checker.scan( logs, CheckType.PROPERTY );
    }

    <C extends Command, R extends Abstract64BitRecord> void scan( File[] logs, CheckType<C,R> check ) throws IOException
    {
        out.println( "Checking logs for " + check.name() + " inconsistencies" );

        ProcessedRecords<R> state = new ProcessedRecords<>( check );

        for ( File log : logs )
        {
            long commandsRead = 0;
            try ( LogEntryIterator logEntryIterator = new LogEntryIterator( fs, log ) )
            {
                while ( logEntryIterator.hasNext() )
                {
                    LogEntry entry = logEntryIterator.next();
                    if ( entry instanceof LogEntryCommand )
                    {
                        Command command = ((LogEntryCommand) entry).getXaCommand();
                        if ( check.commandClass().isInstance( command ) )
                        {
                            process( check.commandClass().cast( command ), check, state, log );
                        }
                    }
                    commandsRead++;
                }
            }
            out.println( "Processed " + log.getCanonicalPath() + " with " + commandsRead + " commands" );
            out.println( state );
        }
    }

    private <C extends Command, R extends Abstract64BitRecord> void process( C command, CheckType<C,R> check,
            ProcessedRecords<R> state, File log )
    {
        long logVersion = PhysicalLogFiles.getLogVersion( log );

        R before = check.before( command );
        R after = check.after( command );

        if ( !state.isValid( before ) )
        {
            LogRecord<R> seen = state.get( before.getId() );
            LogRecord<R> current = new LogRecord<>( before, logVersion );

            inconsistenciesHandler.handle( seen, current );
        }

        state.put( after, logVersion );
    }

    private static File parseDir( String[] args, PrintStream out )
    {
        if ( args.length != 1 )
        {
            printUsageAndExit( out );
        }
        File dir = new File( args[0] );
        if ( !dir.isDirectory() )
        {
            out.println( "Invalid directory" );
            printUsageAndExit( out );
        }
        return dir;
    }

    private static File[] txLogsIn( File dir )
    {
        File[] logs = dir.listFiles( LogFiles.FILENAME_FILTER );
        Arrays.sort( logs, new Comparator<File>()
        {
            @Override
            public int compare( File f1, File f2 )
            {
                long f1Version = PhysicalLogFiles.getLogVersion( f1 );
                long f2Version = PhysicalLogFiles.getLogVersion( f2 );
                return Long.compare( f1Version, f2Version );
            }
        } );
        return logs;
    }

    private static void printUsageAndExit( PrintStream out )
    {
        out.println( "Tool expects single argument - directory with tx logs" );
        System.exit( 1 );
    }

    private static InconsistenciesHandler throwIfInconsistent()
    {
        return new InconsistenciesHandler()
        {
            @Override
            public void handle( LogRecord<?> seen, LogRecord<?> current )
            {
                throw new IllegalStateException(
                        "Before state: " + seen + " is inconsistent with after state: " + current );
            }
        };
    }
}

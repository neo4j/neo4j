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
package org.neo4j.shell.cli;

import jline.console.ConsoleReader;
import jline.console.history.FileHistory;
import jline.console.history.MemoryHistory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

import org.neo4j.shell.Historian;
import org.neo4j.shell.log.Logger;

import static java.lang.System.getProperty;

/**
 * An historian which stores history in a file in the users home dir. The setup methods install a shutdown hook which will flush the history on exit.
 */
public class FileHistorian implements Historian
{

    private final MemoryHistory history;

    private FileHistorian( MemoryHistory history )
    {
        this.history = history;
    }

    @Nonnull
    public static Historian setupHistory( @Nonnull final ConsoleReader reader,
                                          @Nonnull final Logger logger,
                                          @Nonnull final File historyFile ) throws IOException
    {
        try
        {
            File dir = historyFile.getParentFile();
            if ( !dir.isDirectory() && !dir.mkdir() )
            {
                throw new IOException( "Failed to create directory for history: " + dir.getAbsolutePath() );
            }
            final FileHistory history = new FileHistory( historyFile );
            reader.setHistory( history );

            // Make sure we flush history on exit
            addShutdownHookToFlushHistory( logger, history );

            return new FileHistorian( history );
        }
        catch ( IOException e )
        {
            logger.printError( "Could not load history file. Falling back to session-based history.\n"
                               + e.getMessage() );
            MemoryHistory history = new MemoryHistory();
            reader.setHistory( history );
            return new FileHistorian( history );
        }
    }

    private static void addShutdownHookToFlushHistory( @Nonnull final Logger logger, final FileHistory history )
    {
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                try
                {
                    history.flush();
                }
                catch ( IOException e )
                {
                    logger.printError( "Failed to save history:\n" + e.getMessage() );
                }
            }
        } );
    }

    @Nonnull
    public static File getDefaultHistoryFile()
    {
        // Storing in same directory as driver uses
        File dir = new File( getProperty( "user.home" ), ".neo4j" );
        return new File( dir, ".neo4j_history" );
    }

    @Nonnull
    @Override
    public List<String> getHistory()
    {
        List<String> result = new ArrayList<>();

        history.forEach( entry -> result.add( String.valueOf( entry.value() ) ) );

        return result;
    }

    @Override
    public void flushHistory() throws IOException
    {
        if ( history instanceof FileHistory )
        {
            ((FileHistory) history).flush();
        }
    }
}

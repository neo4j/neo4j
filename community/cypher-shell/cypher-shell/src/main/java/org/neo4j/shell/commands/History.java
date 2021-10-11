/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.shell.commands;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.neo4j.shell.Historian;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.exception.ExitException;
import org.neo4j.shell.log.Logger;

import static java.lang.String.format;
import static org.neo4j.shell.commands.CommandHelper.simpleArgParse;

/**
 * Show command history
 */
public class History implements Command
{
    private static final String COMMAND_NAME = ":history";

    private final Logger logger;
    private final Historian historian;
    private final List<String> aliases = Collections.emptyList();

    public History( final Logger logger, final Historian historian )
    {
        this.logger = logger;
        this.historian = historian;
    }

    @Override
    public String getName()
    {
        return COMMAND_NAME;
    }

    @Override
    public String getDescription()
    {
        return "Print a list of the last commands executed";
    }

    @Override
    public String getUsage()
    {
        return "";
    }

    @Override
    public String getHelp()
    {
        return "':history' prints a list of the last statements executed\n':history clear' removes all entries from the history";
    }

    @Override
    public List<String> getAliases()
    {
        return aliases;
    }

    @Override
    public void execute( String argString ) throws ExitException, CommandException
    {
        var args = simpleArgParse( argString, 0, 1, COMMAND_NAME, getUsage() );

        if ( args.length == 1 )
        {
            if ( "clear".equals( args[0] ) )
            {
                clearHistory();
            }
            else
            {
                throw new CommandException( "Unrecognised argument " + args[0] );
            }
        }
        else
        {
            // Calculate starting position
            int lineCount = 16;

            logger.printOut( printHistory( historian.getHistory(), lineCount ) );
        }
    }

    /**
     * Prints N last lines of history.
     *
     * @param lineCount number of entries to print
     */
    private static String printHistory( final List<String> history, final int lineCount )
    {
        // for alignment, check the string length of history size
        int colWidth = Integer.toString( history.size() ).length();
        String firstLineFormat = " %-" + colWidth + "d  %s%n";
        String continuationLineFormat = " %-" + colWidth + "s  %s%n";
        StringBuilder builder = new StringBuilder();

        for ( int i = Math.max( 0, history.size() - lineCount ); i < history.size(); i++ )
        {
            var statement = history.get( i );
            var lines = statement.split( "\\r?\\n" );

            builder.append( format( firstLineFormat, i + 1, lines[0] ) );

            for ( int l = 1; l < lines.length; l++ )
            {
                builder.append( format( continuationLineFormat, " ", lines[l] ) );
            }
        }

        return builder.toString();
    }

    private void clearHistory() throws CommandException
    {
        try
        {
            logger.printIfVerbose( "Removing history..." );
            historian.clear();
        }
        catch ( IOException e )
        {
            throw new CommandException( "Failed to clear history: " + e.getMessage() );
        }
    }
}
